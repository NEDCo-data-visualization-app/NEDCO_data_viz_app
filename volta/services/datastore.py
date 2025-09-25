"""Data access and aggregation helpers."""

from __future__ import annotations
import logging
import os
from io import BytesIO
from typing import Any, Dict, Mapping, Optional, Union

import duckdb
import pandas as pd
import requests

logger = logging.getLogger("volta")


class DataStore:
    """Own data loading, preprocessing, derived stats, and in-memory caching.

    Storage backend: DuckDB (.duckdb file)
    - Source data: CSV files matched by Config.CSV_GLOB
    - Materialized table: prod.sales
    """

    def __init__(self, config: Mapping[str, Any], metrics: "Metrics"):
        self.config = config
        self.metrics = metrics
        self._df: Optional[pd.DataFrame] = None
        self._con: Optional[duckdb.DuckDBPyConnection] = None

    # ---------- DuckDB helpers ----------

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            db_path = str(self.config.get("DUCKDB_PATH"))
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            self._con = duckdb.connect(db_path)
            # optional tuning knobs:
            # self._con.execute("PRAGMA threads = {}".format(max(2, os.cpu_count() or 4)))
        return self._con

    def _table_exists(self) -> bool:
        con = self._connect()
        try:
            return bool(
                con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema='prod' AND table_name='sales';"
                ).fetchone()[0]
            )
        except Exception:
            return False

    def rebuild_from_csv(self) -> None:
        """Full rebuild of prod.sales from CSVs matched by CSV_GLOB."""
        con = self._connect()
        csv_glob = self.config.get("CSV_GLOB", "data/uploads/*.csv")

        con.execute("CREATE SCHEMA IF NOT EXISTS prod;")
        con.execute("DROP TABLE IF EXISTS prod.sales;")

        # If no files match, create an empty table to keep the app happy
        import glob as _glob
        files = _glob.glob(csv_glob)
        if not files:
            logger.warning("No CSV files found for glob %s; creating empty prod.sales", csv_glob)
            con.execute("CREATE TABLE prod.sales AS SELECT * FROM (SELECT 1 AS dummy) WHERE 1=0;")
        else:
            logger.info("Building prod.sales from %d CSV file(s): %s", len(files), csv_glob)
            con.execute(
                f"""
                CREATE TABLE prod.sales AS
                SELECT * FROM read_csv_auto('{csv_glob}', HEADER=TRUE);
                """
            )

        con.execute("ANALYZE prod.sales;")
        logger.info("DuckDB table prod.sales rebuilt and analyzed.")

        # Invalidate in-memory cache
        self._df = None

    # ---------- Existing pandas pipeline compatibility ----------

    @staticmethod
    def _clean_nan_rows(df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(how="any").reset_index(drop=True)

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop_duplicates().reset_index(drop=True)
        df = self._clean_nan_rows(df)

        res_map = self.config.get("RES_MAP", {})
        if "res" in df.columns and res_map:
            df["res_mapped"] = df["res"].astype(str).map(res_map).fillna("Unknown")

        date_col = self.config.get("DATE_COL")
        if (
            date_col
            and date_col in df.columns
            and not pd.api.types.is_datetime64_any_dtype(df[date_col])
        ):
            # original format looked like '%d-%b-%y'
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce", format="%d-%b-%y")

        for numcol in self.metrics.mapping.keys():
            if numcol in df.columns:
                df[numcol] = pd.to_numeric(df[numcol], errors="coerce")

        return df

    def _ensure_data(self) -> None:
        """Make sure the DuckDB has prod.sales; build it if needed."""
        if not self._table_exists():
            logger.info("DuckDB table prod.sales missing; attempting to build from CSV.")
            self.rebuild_from_csv()

    def load(self) -> pd.DataFrame:
        """Load full table from DuckDB (then preprocess) and cache in-memory.

        NOTE: This preserves your current app behavior (a single DataFrame in memory).
        Later we can switch routes to query DuckDB directly for even better performance.
        """
        if self._df is not None:
            return self._df

        # If a remote object store URL is configured for a parquet blob, keep the fallback
        # (optional legacy path—can be removed if you won't use BUCKET_URL anymore)
        url = self.config.get("BUCKET_URL")
        headers = {"apikey": self.config.get("SUPABASE_KEY")}
        raw = None

        if url:
            try:
                resp = requests.get(url, headers=headers, timeout=60)
                resp.raise_for_status()
                raw = pd.read_parquet(BytesIO(resp.content))
                logger.info("Loaded remote parquet from BUCKET_URL into pandas (legacy path).")
            except (requests.HTTPError, requests.ConnectionError, requests.Timeout):
                logger.warning(
                    "Could not fetch remote file. Falling back to DuckDB local table."
                )

        if raw is None:
            # Preferred path: load from DuckDB
            self._ensure_data()
            con = self._connect()
            try:
                raw = con.execute("SELECT * FROM prod.sales;").df()
            except Exception as e:
                logger.error("Failed to read prod.sales from DuckDB: %s", e)
                raw = pd.DataFrame()

        logger.info("Loaded raw DataFrame from backend store")
        self._df = self._preprocess(raw)
        logger.info("Processed DataFrame")
        return self._df

    def set_df(self, df: pd.DataFrame) -> None:
        """Directly set the DataFrame AND update DuckDB (replace prod.sales)."""
        self._df = self._preprocess(df)
        logger.info("DataStore loaded from uploaded file (in-memory).")

        # Also persist to DuckDB so future loads use the DB
        con = self._connect()
        con.execute("CREATE SCHEMA IF NOT EXISTS prod;")
        con.execute("DROP TABLE IF EXISTS prod.sales;")
        con.register("tmp_df", self._df)
        con.execute("CREATE TABLE prod.sales AS SELECT * FROM tmp_df;")
        con.unregister("tmp_df")
        con.execute("ANALYZE prod.sales;")
        logger.info("Persisted uploaded DataFrame into DuckDB prod.sales.")

    def get(self, copy: bool = True) -> pd.DataFrame:
        df = self.load()
        return df.copy(deep=False) if copy else df

    def reload(self) -> None:
        self._df = None
        logger.info("DataStore cache cleared")

    def compute_stats(self, df: pd.DataFrame) -> Dict[str, Dict[str, Union[float, str]]]:
        stats: Dict[str, Dict[str, Union[float, str]]] = {}
        for key, label in self.metrics.mapping.items():
            if key in df.columns:
                s = pd.to_numeric(df[key], errors="coerce").dropna()
                if len(s) > 0:
                    stats[key] = {
                        "label": label,
                        "sum": float(s.sum()),
                        "mean": float(s.mean()),
                        "median": float(s.median()),
                        "min": float(s.min()),
                        "max": float(s.max()),
                    }
        return stats

    def compute_summary(self, df: pd.DataFrame) -> Dict[str, Union[int, str, None]]:
        date_col = self.config.get("DATE_COL")
        out: Dict[str, Union[int, str, None]] = {
            "rows": len(df),
            "cols": len(df.columns),
            "meters": (df["meterid"].nunique() if "meterid" in df.columns else None),
            "locations": (df["loc"].nunique() if "loc" in df.columns else None),
            "date_min": "",
            "date_max": "",
        }
        if date_col and date_col in df.columns and len(df) > 0:
            dmin = pd.to_datetime(df[date_col], errors="coerce").min()
            dmax = pd.to_datetime(df[date_col], errors="coerce").max()
            if pd.notna(dmin):
                out["date_min"] = dmin.date().isoformat()
            if pd.notna(dmax):
                out["date_max"] = dmax.date().isoformat()
        return out


__all__ = ["DataStore"]

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
        csv_glob = self.config.get("CSV_GLOB", "data/*.csv")
        date_col = self.config.get("DATE_COL", "chargedate")
        date_fmt = self.config.get("DATE_FMT", "%d-%b-%y")  # add DATE_FMT to config if needed

        con.execute("CREATE SCHEMA IF NOT EXISTS prod;")
        con.execute("DROP TABLE IF EXISTS prod.sales;")

        import glob as _glob
        files = _glob.glob(csv_glob)
        if not files:
            logger.warning("No CSV files found for glob %s; creating empty prod.sales", csv_glob)
            return
            #con.execute("CREATE TABLE prod.sales AS SELECT * FROM (SELECT 1 AS dummy) WHERE 1=0;")
        else:
            logger.info("Building prod.sales from %d CSV file(s): %s", len(files), csv_glob)
            # Read CSVs, then normalize chargedate to DATE
            con.execute(
                f"""
                CREATE TABLE prod.sales AS
                WITH raw AS (
                  SELECT * FROM read_csv_auto('{csv_glob}', HEADER=TRUE)
                )
                SELECT
                  CAST(try_strptime({date_col}, '{date_fmt}') AS DATE) AS {date_col},
                  * EXCLUDE ({date_col})
                FROM raw;
                """
            )

        con.execute("ANALYZE prod.sales;")
        logger.info("DuckDB table prod.sales rebuilt and analyzed.")

        # Optional: helpful indexes
        con.execute("CREATE INDEX IF NOT EXISTS idx_sales_country ON prod.sales(country);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_sales_category ON prod.sales(category);")

        self._df = None

    def run_query(self, sql: str, params=None) -> pd.DataFrame:
        """Execute SQL on DuckDB and return as pandas DataFrame."""
        con = self._connect()
        return con.execute(sql, params or []).df()

    def timeseries_daily(self, date_from, date_to, country=None, category=None) -> pd.DataFrame:
        sql = f"""
        SELECT
          date_trunc('day', {self.config.get("DATE_COL", "chargedate")}) AS day,
          SUM(amount) AS total_amount
        FROM prod.sales
        WHERE {self.config.get("DATE_COL", "chargedate")} BETWEEN ? AND ?
          AND (? IS NULL OR country = ?)
          AND (? IS NULL OR category = ?)
        GROUP BY 1
        ORDER BY 1;
        """
        params = [date_from, date_to, country, country, category, category]
        return self.run_query(sql, params)

    def top_categories(self, date_from, date_to, limit=10) -> pd.DataFrame:
        sql = f"""
        SELECT
          category,
          SUM(amount) AS total_amount
        FROM prod.sales
        WHERE {self.config.get("DATE_COL", "chargedate")} BETWEEN ? AND ?
        GROUP BY category
        ORDER BY total_amount DESC
        LIMIT ?;
        """
        return self.run_query(sql, [date_from, date_to, limit])

    def table_page(self, date_from, date_to, country=None, limit=100, offset=0) -> pd.DataFrame:
        sql = f"""
        SELECT {self.config.get("DATE_COL", "chargedate")} AS chargedate,
               country, category, amount
        FROM prod.sales
        WHERE {self.config.get("DATE_COL", "chargedate")} BETWEEN ? AND ?
          AND (? IS NULL OR country = ?)
        ORDER BY {self.config.get("DATE_COL", "chargedate")} DESC
        LIMIT ? OFFSET ?;
        """
        params = [date_from, date_to, country, country, limit, offset]
        return self.run_query(sql, params)

    # ---------- Existing pandas compatibility ----------

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
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce", format="%d-%b-%y")

        for numcol in self.metrics.mapping.keys():
            if numcol in df.columns:
                df[numcol] = pd.to_numeric(df[numcol], errors="coerce")

        return df

    def _ensure_data(self) -> None:
        if not self._table_exists():
            logger.info("DuckDB table prod.sales missing; attempting to build from CSV.")
            self.rebuild_from_csv()

    def load(self):
        if self._df is not None:
            return self._df

        con = self._connect()
        if self._table_exists():
            try:
                raw = con.execute("SELECT * FROM prod.sales;").df()
                logger.info("Loaded data from local DuckDB prod.sales.")
                self._df = self._preprocess(raw)
                return self._df
            except Exception as e:
                logger.warning("DuckDB table load failed: %s", e)
        url = self.config.get("BUCKET_URL")
        headers = {"apikey": self.config.get("SUPABASE_KEY")}
        if url:
            try:
                resp = requests.get(url, headers=headers, timeout=60)
                resp.raise_for_status()
                raw = pd.read_parquet(BytesIO(resp.content))
                logger.info("Loaded remote parquet from BUCKET_URL.")
                self.set_df(raw)
                logger.info("Set and persisted remote data into DuckDB.")
                return self._df
            except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
                logger.error("Failed to fetch remote file from BUCKET_URL: %s", e)

        logger.error("No data source succeeded; skipping DuckDB and CSV fallback.")
        self._df = None
        return pd.DataFrame()
    
    def try_internet_connection(self) -> bool:
        url = self.config.get("BUCKET_URL")
        headers = {"apikey": self.config.get("SUPABASE_KEY")}
        if not url:
            logger.warning("No BUCKET_URL configured.")
            return False

        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            raw = pd.read_parquet(BytesIO(resp.content))
            logger.info("Internet connection successful. Remote data fetched.")
            self.set_df(raw)
            return True
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
            logger.warning("Internet connection failed: %s", e)
            return False

    def set_df(self, df: pd.DataFrame) -> None:
        self._df = self._preprocess(df)
        logger.info("DataStore loaded from uploaded file (in-memory).")

        date_col = self.config.get("DATE_COL", "chargedate")

        con = self._connect()
        con.execute("CREATE SCHEMA IF NOT EXISTS prod;")
        con.execute("DROP TABLE IF EXISTS prod.sales;")
        con.register("tmp_df", self._df)

        # Ensure the persisted column is DATE (or TIMESTAMP) and drop the old one
        con.execute(f"""
            CREATE TABLE prod.sales AS
            SELECT
              CAST({date_col} AS DATE) AS {date_col},
              * EXCLUDE ({date_col})
            FROM tmp_df;
        """)
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

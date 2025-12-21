"""Data access and aggregation helpers - query Parquet only, no caching or loading."""

from __future__ import annotations
import logging
from typing import Any, Dict, Mapping, Union
import duckdb
from .metrics import Metrics
from flask import current_app

logger = logging.getLogger("volta")


class DataStore:
    """DataStore that queries a Parquet file directly with DuckDB without storing anything in memory."""

    def __init__(self, config: Mapping[str, Any], metrics: Metrics):
        self.config = config
        self.metrics = metrics
        self._columns: list[str] | None = None 
        self.parquet_path = self.config.get(
            "PARQUET_PATH",
            "/Users/srinandham/Downloads/NEDCO_data_viz_app/data/test.parquet",
        )
        self.date_col = self.config.get("DATE_COL", "od_date")

    # ---------- DuckDB helpers ----------

    def _connect(self) -> duckdb.DuckDBPyConnection:
        """Return a fresh in-memory DuckDB connection."""
        return duckdb.connect(database=":memory:")
    
    def get_columns(self) -> list[str]:
        """Get column names from Parquet (cached)."""
        if self._columns is None:
            con = self._connect()
            con.execute(f"DESCRIBE '{self.parquet_path}'")
            self._columns = [row[0] for row in con.fetchall()]
            con.close()
        return self._columns

    def run_query(self, sql: str, params=None) -> list[dict[str, Any]]:
        """Execute SQL and return results as list-of-dicts."""
        try:
            con = self._connect()
            cur = con.execute(sql, params or [])
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
            con.close()
            return [dict(zip(cols, r)) for r in rows]
        except Exception as e:
            logger.error("DuckDB query failed: %s", e)
            return []

    # ---------- Timeseries & table ----------

    def timeseries_daily(self, date_from, date_to, country=None, category=None):
        sql = f"""
        SELECT
            date_trunc('day', {self.date_col}) AS day,
            SUM(amount) AS total_amount
        FROM '{self.parquet_path}'
        WHERE {self.date_col} BETWEEN ? AND ?
          AND (? IS NULL OR country = ?)
          AND (? IS NULL OR category = ?)
        GROUP BY 1
        ORDER BY 1;
        """
        params = [date_from, date_to, country, country, category, category]
        return self.run_query(sql, params)

    def top_categories(self, date_from, date_to, limit=10):
        sql = f"""
        SELECT
            category,
            SUM(amount) AS total_amount
        FROM '{self.parquet_path}'
        WHERE {self.date_col} BETWEEN ? AND ?
        GROUP BY category
        ORDER BY total_amount DESC
        LIMIT ?;
        """
        return self.run_query(sql, [date_from, date_to, limit])

    def table_page(self, date_from, date_to, country=None, limit=100, offset=0):
        sql = f"""
        SELECT
            {self.date_col} AS od_date,
            country, category, amount
        FROM '{self.parquet_path}'
        WHERE {self.date_col} BETWEEN ? AND ?
          AND (? IS NULL OR country = ?)
        ORDER BY {self.date_col} DESC
        LIMIT ? OFFSET ?;
        """
        params = [date_from, date_to, country, country, limit, offset]
        return self.run_query(sql, params)

    # ---------- Stats / summary ----------

    def compute_stats(self, rows: list[dict[str, Any]]) -> Dict[str, Dict[str, Union[float, str]]]:
        """Compute sum, mean, median, min, max for available metrics using Metrics class."""
        stats: Dict[str, Dict[str, Union[float, str]]] = {}
        for key in self.metrics.keys(rows):
            vals = [r[key] for r in rows if r.get(key) is not None]
            if vals:
                n = len(vals)
                sorted_vals = sorted(vals)
                median = sorted_vals[n // 2] if n % 2 == 1 else (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
                stats[key] = {
                    "label": self.metrics.label(key),
                    "sum": float(sum(vals)),
                    "mean": float(sum(vals) / n),
                    "median": float(median),
                    "min": float(min(vals)),
                    "max": float(max(vals)),
                }
        return stats

    def compute_summary(self, rows: list[dict[str, Any]]) -> Dict[str, Union[int, str, None]]:
        """Compute dataset summary for list-of-dicts dataset."""
        out: Dict[str, Union[int, str, None]] = {
            "rows": len(rows),
            "cols": len(rows[0]) if rows else 0,
            "meters": len({r["meterid"] for r in rows if "meterid" in r}) if rows else 0,
            "locations": len({r["utility"] for r in rows if "utility" in r}) if rows else 0,
            "date_min": "",
            "date_max": "",
        }
        if rows and self.date_col:
            dates = [r[self.date_col] for r in rows if r.get(self.date_col)]
            if dates:
                sorted_dates = sorted(dates)
                out["date_min"] = str(sorted_dates[0])
                out["date_max"] = str(sorted_dates[-1])
        return out


__all__ = ["DataStore"]

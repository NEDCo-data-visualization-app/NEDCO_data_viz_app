from __future__ import annotations
import logging
from typing import Any, Dict, Mapping, Union
import duckdb
import pyarrow as pa
from .metrics import Metrics
from flask import current_app

logger = logging.getLogger("volta")


class DataStore:
    """DataStore that queries a Parquet file directly using a persistent DuckDB connection.
    Now supports streaming results to avoid large memory usage.
    """

    def __init__(self, config: Mapping[str, Any], metrics: Metrics):
        self.config = config
        self.metrics = metrics
        self._columns: list[str] | None = None
        self.parquet_path = self.config["PARQUET_PATH"]
        self._con = duckdb.connect(database=self.config["DB_PATH"], read_only=False)
        self.date_col = self.config.get("DATE_COL", "od_date")

    def get_columns(self) -> list[str]:
        """Cache columns instead of fetching every time."""
        if self._columns is None:
            sql = f'SELECT * FROM "{self.parquet_path}" LIMIT 1'
            row_gen = self.run_query(sql, fetch_all=False)  # returns generator
            first_row = next(row_gen, None)  # get first row safely
            self._columns = list(first_row.keys()) if first_row else []
        return self._columns



    def run_query(self, sql: str, params=None, fetch_all=True):
        """
        Execute SQL and return results as:
        - generator (if fetch_all=False)
        - list of dicts (if fetch_all=True)
        """
        try:
            cur = self._con.execute(sql, params or [])
            cols = [c[0] for c in cur.description]

            if fetch_all:
                rows = cur.fetchall()
                return [dict(zip(cols, r)) for r in rows]

            # Memory-safe generator
            def row_generator():
                for r in cur.fetchall():
                    yield dict(zip(cols, r))
            return row_generator()

        except Exception as e:
            logger.error("DuckDB query failed: %s", e)
            if fetch_all:
                return []
            else:
                return iter([])  # empty generator



    # ---------- Example Timeseries & Table Queries ----------

    def timeseries_daily(self, date_from, date_to, country=None, category=None):
        sql = f"""
        SELECT
            date_trunc('day', {self.date_col}) AS day,
            SUM(amount) AS total_amount
        FROM "{self.parquet_path}"
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
        FROM "{self.parquet_path}"
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
        FROM "{self.parquet_path}"
        WHERE {self.date_col} BETWEEN ? AND ?
          AND (? IS NULL OR country = ?)
        ORDER BY {self.date_col} DESC
        LIMIT ? OFFSET ?;
        """
        params = [date_from, date_to, country, country, limit, offset]
        return self.run_query(sql, params)

    # ---------- Stats / summary (SQL-based) ----------

    def compute_stats(self, where_clause: str = "", sql_params: list = None) -> Dict[str, Dict[str, Union[float, str]]]:
        """
        Compute stats (sum, mean, min, max, median) directly in SQL for all metrics.
        This avoids loading all rows into Python memory.
        """
        metrics = self.metrics.keys() 
        if not metrics:
            return {}

        sql_parts = []
        for metric in metrics:
            # SUM, AVG, MIN, MAX
            sql_parts.append(f"""
                SUM({metric}) AS sum_{metric},
                AVG({metric}) AS avg_{metric},
                MIN({metric}) AS min_{metric},
                MAX({metric}) AS max_{metric},
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {metric}) AS median_{metric}
            """)

        sql = f'SELECT {", ".join(sql_parts)} FROM "{self.parquet_path}"'
        if where_clause:
            sql += f" WHERE {where_clause}"

        try:
            result = self.run_query(sql, sql_params or [], fetch_all=True)
            if not result:
                return {}

            row = result[0]
            stats: Dict[str, Dict[str, Union[float, str]]] = {}
            for metric in metrics:
                stats[metric] = {
                    "label": self.metrics.label(metric),
                    "sum": float(row.get(f"sum_{metric}") or 0),
                    "mean": float(row.get(f"avg_{metric}") or 0),
                    "median": float(row.get(f"median_{metric}") or 0),
                    "min": float(row.get(f"min_{metric}") or 0),
                    "max": float(row.get(f"max_{metric}") or 0),
                }
            return stats
        except Exception as e:
            logger.exception("Failed to compute stats in SQL: %s", e)
            return {}


    def compute_summary(self, where_clause: str = "", sql_params: list = None) -> Dict[str, Union[int, str, None]]:
        """
        Compute summary (row count, distinct meters/locations, min/max date) in SQL.
        """
        date_col = self.date_col
        sql = f'''
            SELECT
                COUNT(*) AS n_rows,
                COUNT(DISTINCT meterid) AS meters,
                COUNT(DISTINCT utility) AS locations,
                MIN({date_col}) AS date_min,
                MAX({date_col}) AS date_max
            FROM "{self.parquet_path}"
        '''
        if where_clause:
            sql += f" WHERE {where_clause}"

        try:
            result = self.run_query(sql, sql_params or [], fetch_all=True)
            if not result:
                return {"rows": 0, "cols": 0, "meters": 0, "locations": 0, "date_min": "", "date_max": ""}

            row = result[0]
            # cols can still be fetched from get_columns()
            return {
                "rows": int(row.get("n_rows") or 0),
                "cols": len(self.get_columns()),
                "meters": int(row.get("meters") or 0),
                "locations": int(row.get("locations") or 0),
                "date_min": str(row.get("date_min") or ""),
                "date_max": str(row.get("date_max") or ""),
            }
        except Exception as e:
            logger.exception("Failed to compute summary in SQL: %s", e)
            return {"rows": 0, "cols": 0, "meters": 0, "locations": 0, "date_min": "", "date_max": ""}


__all__ = ["DataStore"]

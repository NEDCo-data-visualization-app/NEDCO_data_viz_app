from __future__ import annotations
import logging
from typing import Any, Dict, Mapping, Optional, Union
import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger("volta")
TABLE_NAME = "volta-test-481721.test123.table_test"


class DataStore:
    """Query-only BigQuery-backed datastore; no local caching."""

    def __init__(self, config: Mapping[str, Any], metrics: "Metrics"):
        self.config = config
        self.metrics = metrics
        self._client: Optional[bigquery.Client] = None
        self.TABLE_NAME = config.get("TABLE_NAME", "volta-test-481721.test123.table_test")

    def _connect(self) -> bigquery.Client:
        if self._client is None:
            self._client = bigquery.Client()
        return self._client

    def run_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        client = self._connect()
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(k, "STRING", v)
                for k, v in (params or {}).items()
            ]
        )
        query_job = client.query(sql, job_config=job_config)
        return query_job.result().to_dataframe()

    # Example queries

    def timeseries_daily(self, date_from, date_to, country=None, category=None) -> pd.DataFrame:
        sql = f"""
        SELECT
            DATE_TRUNC({self.config.get("DATE_COL", "od_date")}, DAY) AS day,
            SUM(amount) AS total_amount
        FROM `{TABLE_NAME}`
        WHERE {self.config.get("DATE_COL", "od_date")} BETWEEN @date_from AND @date_to
          AND (@country IS NULL OR country = @country)
          AND (@category IS NULL OR category = @category)
        GROUP BY day
        ORDER BY day
        """
        params = {"date_from": date_from, "date_to": date_to, "country": country, "category": category}
        return self.run_query(sql, params)

    def top_categories(self, date_from, date_to, limit=10) -> pd.DataFrame:
        sql = f"""
        SELECT
            category,
            SUM(amount) AS total_amount
        FROM `{TABLE_NAME}`
        WHERE {self.config.get("DATE_COL", "od_date")} BETWEEN @date_from AND @date_to
        GROUP BY category
        ORDER BY total_amount DESC
        LIMIT @limit
        """
        params = {"date_from": date_from, "date_to": date_to, "limit": limit}
        return self.run_query(sql, params)

    def table_page(self, date_from, date_to, country=None, limit=100, offset=0) -> pd.DataFrame:
        sql = f"""
        SELECT {self.config.get("DATE_COL", "od_date")} AS od_date,
               country, category, amount
        FROM `{TABLE_NAME}`
        WHERE {self.config.get("DATE_COL", "od_date")} BETWEEN @date_from AND @date_to
          AND (@country IS NULL OR country = @country)
        ORDER BY {self.config.get("DATE_COL", "od_date")} DESC
        LIMIT @limit OFFSET @offset
        """
        params = {"date_from": date_from, "date_to": date_to, "country": country, "limit": limit, "offset": offset}
        return self.run_query(sql, params)

    # Stats are computed on-demand via SQL
    def compute_stats(self, date_from, date_to) -> pd.DataFrame:
        sql = f"""
        SELECT
            SUM(ocd_energy) AS energy_sum,
            AVG(ocd_energy) AS energy_avg,
            MIN(ocd_energy) AS energy_min,
            MAX(ocd_energy) AS energy_max
        FROM `{TABLE_NAME}`
        WHERE {self.config.get("DATE_COL", "od_date")} BETWEEN @date_from AND @date_to
        """
        return self.run_query(sql, {"date_from": date_from, "date_to": date_to})


__all__ = ["DataStore"]

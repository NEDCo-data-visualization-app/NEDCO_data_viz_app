"""Shared helpers for dashboard routes."""

from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional, Sequence

from flask import current_app

from volta.utils.filter_params import FilterParams

DEFAULT_METERID_LIMIT = 500
RESERVED_ARGS = {"start_date", "end_date", "freq", "metric", "split_by", "agg"}


def _parse_date(value: str) -> Optional[date]:
    """Parse ``YYYY-MM-DD`` (or any ISO form); return None when empty or invalid."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            return None


def build_params(args, base_columns: Sequence[str]) -> FilterParams:
    """Build FilterParams from request args (dates, freq, metric and column selections)."""
    freq = (args.get("freq") or "M").upper()
    if freq not in ("D", "W", "M"):
        freq = "M"

    metric = (args.get("metric") or "").strip() or None  # may be comma separated

    columns_lc = {c.lower(): c for c in base_columns}
    selections: Dict[str, List[str]] = {}
    for key in args.keys():
        if key in RESERVED_ARGS:
            continue
        actual = columns_lc.get(key.lower())
        if not actual:
            continue
        values = [str(v) for v in args.getlist(key) if v]
        if values:
            selections[actual] = values

    return FilterParams(
        start=_parse_date((args.get("start_date") or "").strip()),
        end=_parse_date((args.get("end_date") or "").strip()),
        selections=selections,
        freq=freq,
        metric=metric,
    )


def build_unique_values(
    datastore,
    columns: List[str],
    clause: str = "",
    sql_params: Optional[List] = None,
    max_uniques: Optional[int] = None,
) -> Dict[str, List[str]]:
    """Distinct values for each column (skipping excluded ones), computed in DuckDB."""
    exclude_cols = current_app.config.get("EXCLUDE_COLS", set())
    available = set(datastore.get_columns())
    unique: Dict[str, List[str]] = {}

    for column in columns:
        if column in exclude_cols or column not in available:
            continue
        sql = f"SELECT DISTINCT CAST({column} AS VARCHAR) AS v FROM {datastore.table_sql} WHERE {column} IS NOT NULL"
        if clause:
            sql += f" AND {clause}"
        sql += " ORDER BY v"
        if max_uniques:
            sql += f" LIMIT {int(max_uniques)}"
        unique[column] = [str(r["v"]) for r in datastore.run_query(sql, sql_params)]

    return unique


__all__ = ["DEFAULT_METERID_LIMIT", "_parse_date", "build_params", "build_unique_values"]

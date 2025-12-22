"""Shared helper functions for dashboard routes (DuckDB + Parquet, no pandas)."""

from __future__ import annotations
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

from flask import current_app

from volta.utils.filter_params import FilterParams

DEFAULT_METERID_LIMIT = 500


def _parse_date(value: str) -> Optional[date]:
    """Parse a date string to a datetime.date, robust to empty or ISO formats."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        try:
            parsed = datetime.fromisoformat(value)
            return parsed.date()
        except ValueError:
            return None


def build_params(args, base_columns: Optional[List[str]] = None) -> FilterParams:
    """
    Build FilterParams from request args, case-insensitive columns.
    base_columns: list of column names (optional)
    """
    selections: Dict[str, List[str]] = {}
    exclude_cols = current_app.config.get("EXCLUDE_COLS", set())
    cols_lc = {str(c).lower(): c for c in (base_columns or [])}

    if base_columns:
        for column in base_columns:
            if column in exclude_cols:
                continue
            values = args.getlist(column) or args.getlist(str(column).lower())
            if values:
                selections[column] = [str(v) for v in values]

    for key in args.keys():
        if key in selections:
            continue
        real_col = cols_lc.get(str(key).lower())
        if real_col and real_col not in exclude_cols:
            vals = args.getlist(key)
            if vals:
                selections[real_col] = [str(v) for v in vals]

    freq = (args.get("freq") or "D").upper()
    if freq not in ("D", "W", "M"):
        freq = "D"

    metric = args.get("metric") or None

    return FilterParams(
        start=_parse_date(args.get("start_date", "")),
        end=_parse_date(args.get("end_date", "")),
        selections=selections,
        freq=freq,
        metric=metric,
    )



def build_unique_values(
    datastore,
    columns: List[str],
    clause: str = "",
    sql_params: Optional[List] = None,
    max_uniques: Optional[int] = None,  # optional
) -> Dict[str, List[str]]:
    """
    Build unique values for given columns by querying DuckDB.
    This mimics df[column].unique() in Pandas without loading all rows.
    """
    unique: Dict[str, List[str]] = {}
    exclude_cols = current_app.config.get("EXCLUDE_COLS", set())

    for column in columns:
        if column in exclude_cols:
            continue

        sql = f'SELECT DISTINCT CAST({column} AS VARCHAR) AS v FROM "{current_app.config["PARQUET_PATH"]}"'
        if clause:
            sql += f" WHERE {clause}"
        sql += " ORDER BY v"

        rows = datastore.run_query(sql, sql_params, fetch_all=False)
        values = [str(r["v"]) for r in rows]

        if max_uniques:
            values = values[:max_uniques]

        unique[column] = values

    return unique



def get_base_date_bounds(datastore, date_col: str) -> Tuple[str, str]:
    """
    Get min and max dates directly from DuckDB.
    """
    parquet_table = f'"{current_app.config["PARQUET_PATH"]}"'
    sql = f'''
    SELECT
        MIN(CAST({date_col} AS DATE)) AS dmin,
        MAX(CAST({date_col} AS DATE)) AS dmax
    FROM {parquet_table}
    '''

    rows = datastore.run_query(sql)
    if not rows:
        return "", ""
    dmin = rows[0]["dmin"].isoformat() if rows[0]["dmin"] else ""
    dmax = rows[0]["dmax"].isoformat() if rows[0]["dmax"] else ""
    return dmin, dmax


def no_filters_selected(args, datastore, columns: List[str], date_col: str) -> bool:
    """
    Determine if any filters are applied based on request args and DuckDB table.
    """
    exclude_cols = current_app.config.get("EXCLUDE_COLS", set())

    # Check for any selection filters in args
    for col in columns:
        if col in exclude_cols:
            continue
        if args.getlist(col) or args.getlist(col.lower()):
            return False

    # Check if date filters differ from base table bounds
    base_min, base_max = get_base_date_bounds(datastore, date_col)
    start_in = args.get("start_date", "")
    end_in = args.get("end_date", "")
    if not start_in and not end_in:
        return True
    if (start_in == base_min or not start_in) and (end_in == base_max or not end_in):
        return True
    return False


__all__ = [
    "DEFAULT_METERID_LIMIT",
    "_parse_date",
    "build_params",
    "build_unique_values",
    "get_base_date_bounds",
    "no_filters_selected",
]

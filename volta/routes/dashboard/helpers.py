"""Shared helper functions for dashboard routes."""

from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from flask import current_app

from volta.utils.filter_params import FilterParams

DEFAULT_METERID_LIMIT = 500


def _parse_date(value: str) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        parsed = pd.to_datetime(value, errors="coerce")
        return parsed.date() if pd.notna(parsed) else None


def build_params(args, base_df: pd.DataFrame) -> FilterParams:
    """Build ``FilterParams`` from request args with case-insensitive columns."""
    selections: Dict[str, List[str]] = {}
    exclude_cols = current_app.config["EXCLUDE_COLS"]

    cols_lc = {str(c).lower(): c for c in base_df.columns}

    for column in base_df.columns:
        if column in exclude_cols:
            continue
        values = args.getlist(column)
        if not values:
            values = args.getlist(str(column).lower())
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


def build_unique_values(df: pd.DataFrame, max_uniques: int = 200) -> Dict[str, List[str]]:
    unique: Dict[str, List[str]] = {}
    exclude_cols = current_app.config["EXCLUDE_COLS"]
    for column in df.columns:
        if column in exclude_cols:
            continue
        values = pd.Series(df[column].dropna().unique()).astype(str).tolist()
        values = sorted(set(values))[:max_uniques]
        unique[column] = values
    return unique


def get_base_date_bounds(df: pd.DataFrame) -> Tuple[str, str]:
    date_col = current_app.config["DATE_COL"]
    if date_col not in df.columns or len(df) == 0:
        return "", ""
    dmin = pd.to_datetime(df[date_col], errors="coerce").min()
    dmax = pd.to_datetime(df[date_col], errors="coerce").max()
    start = dmin.date().isoformat() if pd.notna(dmin) else ""
    end = dmax.date().isoformat() if pd.notna(dmax) else ""
    return start, end


def no_filters_selected(args, base_df: pd.DataFrame) -> bool:
    exclude_cols = current_app.config["EXCLUDE_COLS"]
    any_checkbox = any(
        args.getlist(column) for column in base_df.columns if column not in exclude_cols
    )
    if any_checkbox:
        return False
    base_min, base_max = get_base_date_bounds(base_df)
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
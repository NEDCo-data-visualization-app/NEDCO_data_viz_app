"""Filter endpoints for dashboard."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd
from flask import current_app, jsonify, request

from . import bp, get_datastore
from .helpers import DEFAULT_METERID_LIMIT, _parse_date
from volta.utils.filter_params import FilterParams


@bp.route("/filters/options", methods=["POST"])
def filter_options():
    datastore = get_datastore()
    base = datastore.get(copy=False)
    if base.empty:
        return jsonify({"options": {}, "dates": {"min": "", "max": ""}, "rows": 0})

    payload = request.get_json(silent=True) or {}

    cols_lc = {str(c).lower(): c for c in base.columns}

    raw_selections = payload.get("selections") or {}
    selections: Dict[str, List[str]] = {}
    for in_key, values in raw_selections.items():
        if not isinstance(values, (list, tuple)):
            continue
        real_col = cols_lc.get(str(in_key).lower())
        if not real_col:
            continue
        cleaned = [str(v) for v in values if v not in (None, "")]
        if cleaned:
            selections[real_col] = cleaned

    facets_in = payload.get("facets") or []
    if facets_in:
        facets = [
            cols_lc.get(str(f).lower())
            for f in facets_in
            if cols_lc.get(str(f).lower()) in base.columns
        ]
    else:
        facets = [c for c in ["loc", "res_mapped", "meterid"] if c in base.columns]

    params = FilterParams(
        start=_parse_date(str(payload.get("start_date") or "")),
        end=_parse_date(str(payload.get("end_date") or "")),
        selections=selections,
        freq=(payload.get("freq") or "D").upper(),
        metric=payload.get("metric") or None,
    )

    date_col = current_app.config["DATE_COL"]
    clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=base.columns)

    def distinct(col: str) -> List[str]:
        df = datastore.run_query(
            f"""
            SELECT DISTINCT CAST({col} AS VARCHAR) AS v
            FROM prod.sales
            WHERE {clause} AND {col} IS NOT NULL
            ORDER BY v
            """,
            sql_params,
        )
        if df is None or df.empty:
            return []
        return df["v"].astype(str).tolist()

    unique_values: Dict[str, List[str]] = {}
    for col in facets:
        unique_values[col] = distinct(col)

    meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)
    if "meterid" in unique_values:
        unique_values["meterid"] = unique_values["meterid"][: int(meter_cap)]

    ddf = datastore.run_query(
        f"""
        SELECT
          MIN(CAST({date_col} AS DATE)) AS dmin,
          MAX(CAST({date_col} AS DATE)) AS dmax
        FROM prod.sales
        WHERE {clause}
        """,
        sql_params,
    )
    date_min = (
        ddf.iloc[0]["dmin"].isoformat() if ddf is not None and pd.notna(ddf.iloc[0]["dmin"]) else ""
    )
    date_max = (
        ddf.iloc[0]["dmax"].isoformat() if ddf is not None and pd.notna(ddf.iloc[0]["dmax"]) else ""
    )

    cdf = datastore.run_query(
        f"SELECT COUNT(*) AS n FROM prod.sales WHERE {clause};",
        sql_params,
    )
    rows = int(cdf.iloc[0]["n"]) if cdf is not None else 0

    return jsonify(
        {
            "options": unique_values,
            "dates": {"min": date_min, "max": date_max},
            "rows": rows,
        }
    )
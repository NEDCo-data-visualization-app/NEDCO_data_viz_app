"""Live filter options: distinct values per facet given the current selections."""

from __future__ import annotations

from typing import Dict, List

from flask import current_app, jsonify, request

from volta.utils.filter_params import FilterParams

from . import bp, get_datastore
from .helpers import DEFAULT_METERID_LIMIT, _parse_date

DEFAULT_FACETS = ["utility", "tariff_type", "meterid"]


@bp.route("/filters/options", methods=["POST"])
def filter_options():
    datastore = get_datastore()
    payload = request.get_json(silent=True) or {}
    exclude_cols = current_app.config.get("EXCLUDE_COLS", set())

    base_cols = datastore.get_columns()
    if not base_cols:
        return jsonify({"options": {}, "dates": {"min": "", "max": ""}, "rows": 0})
    cols_lc = {str(c).lower(): c for c in base_cols if c not in exclude_cols}

    selections: Dict[str, List[str]] = {}
    for in_key, values in (payload.get("selections") or {}).items():
        if not isinstance(values, (list, tuple)):
            continue
        real_col = cols_lc.get(str(in_key).lower())
        cleaned = [str(v) for v in values if v not in (None, "")]
        if real_col and cleaned:
            selections[real_col] = cleaned

    facets: Dict[str, str] = {}
    for f in payload.get("facets") or DEFAULT_FACETS:
        real = cols_lc.get(str(f).lower())
        if real:
            facets[str(f)] = real

    params = FilterParams(
        start=_parse_date(str(payload.get("start_date") or "")),
        end=_parse_date(str(payload.get("end_date") or "")),
        selections=selections,
    )
    date_col = current_app.config["DATE_COL"]
    clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=base_cols)
    meter_cap = int(current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT))

    options: Dict[str, List[str]] = {}
    for display_col, real_col in facets.items():
        sql = (
            f"SELECT DISTINCT CAST({real_col} AS VARCHAR) AS v FROM {datastore.table_sql} "
            f"WHERE {clause} AND {real_col} IS NOT NULL ORDER BY v"
        )
        if real_col == "meterid":
            sql += f" LIMIT {meter_cap}"
        options[display_col] = [str(r["v"]) for r in datastore.run_query(sql, sql_params)]

    dates = datastore.fetch_one(
        f"SELECT MIN(CAST({date_col} AS DATE)) AS dmin, MAX(CAST({date_col} AS DATE)) AS dmax "
        f"FROM {datastore.table_sql} WHERE {clause}",
        sql_params,
    ) or {}
    count = datastore.fetch_one(f"SELECT COUNT(*) AS n FROM {datastore.table_sql} WHERE {clause}", sql_params) or {}

    return jsonify(
        {
            "options": options,
            "dates": {
                "min": dates["dmin"].isoformat() if dates.get("dmin") else "",
                "max": dates["dmax"].isoformat() if dates.get("dmax") else "",
            },
            "rows": int(count.get("n") or 0),
        }
    )

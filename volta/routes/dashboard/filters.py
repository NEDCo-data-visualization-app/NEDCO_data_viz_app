"""Filter endpoints for dashboard (DuckDB + Parquet, no pandas)."""

from __future__ import annotations
from typing import Dict, List

from flask import current_app, jsonify, request

from . import bp, get_datastore
from .helpers import DEFAULT_METERID_LIMIT, _parse_date
from volta.utils.filter_params import FilterParams


@bp.route("/filters/options", methods=["POST"])
def filter_options():
    datastore = get_datastore()

    payload = request.get_json(silent=True) or {}

    exclude_cols = current_app.config.get("EXCLUDE_COLS", set())

    # Use first row to determine columns
    probe = datastore.run_query(f"SELECT * FROM '{current_app.config["PARQUET_PATH"]}' LIMIT 1")
    if not probe:
        return jsonify({"options": {}, "dates": {"min": "", "max": ""}, "rows": 0})

    base_cols = list(probe[0].keys())
    cols_lc = {str(c).lower(): c for c in base_cols if c not in exclude_cols}

    # ---------- Parse selections ----------
    raw_selections = payload.get("selections") or {}
    selections: Dict[str, List[str]] = {}
    for in_key, values in raw_selections.items():
        if not isinstance(values, (list, tuple)):
            continue
        real_col = cols_lc.get(str(in_key).lower())
        if not real_col or real_col in exclude_cols:
            continue
        cleaned = [str(v) for v in values if v not in (None, "")]
        if cleaned:
            selections[real_col] = cleaned

    # ---------- Determine facets ----------
    facets_in = payload.get("facets") or []
    resolved_facets: Dict[str, str] = {}
    if facets_in:
        for f in facets_in:
            real = cols_lc.get(str(f).lower())
            if real:
                resolved_facets[str(f)] = real
    else:
        for candidate in ["utility", "tariff_type", "meterid"]:
            if candidate in base_cols:
                resolved_facets[candidate] = candidate

    # ---------- Build FilterParams ----------
    params = FilterParams(
        start=_parse_date(str(payload.get("start_date") or "")),
        end=_parse_date(str(payload.get("end_date") or "")),
        selections=selections,
        freq=(payload.get("freq") or "D").upper(),
        metric=payload.get("metric") or None,
    )

    date_col = current_app.config["DATE_COL"]
    clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=base_cols)

    # ---------- Helper to get distinct values ----------
    def distinct(col: str) -> List[str]:
        rows = datastore.run_query(
            f"""
            SELECT DISTINCT CAST({col} AS VARCHAR) AS v
            FROM '{current_app.config["PARQUET_PATH"]}'
            WHERE {clause} AND {col} IS NOT NULL
            ORDER BY v
            """,
            sql_params,
        )
        return [str(r["v"]) for r in rows] if rows else []

    unique_values: Dict[str, List[str]] = {}
    for display_col, real_col in resolved_facets.items():
        unique_values[display_col] = distinct(real_col)

    meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)
    if "meterid" in unique_values:
        unique_values["meterid"] = unique_values["meterid"][: int(meter_cap)]

    # ---------- Min/max date ----------
    date_rows = datastore.run_query(
        f"""
        SELECT
          MIN(CAST({date_col} AS DATE)) AS dmin,
          MAX(CAST({date_col} AS DATE)) AS dmax
        FROM '{current_app.config["PARQUET_PATH"]}'
        WHERE {clause}
        """,
        sql_params,
    )
    date_min = date_rows[0]["dmin"].isoformat() if date_rows and date_rows[0]["dmin"] else ""
    date_max = date_rows[0]["dmax"].isoformat() if date_rows and date_rows[0]["dmax"] else ""

    # ---------- Row count ----------
    count_rows = datastore.run_query(
        f"SELECT COUNT(*) AS n FROM '{current_app.config["PARQUET_PATH"]}' WHERE {clause};",
        sql_params,
    )
    rows = int(count_rows[0]["n"]) if count_rows else 0

    return jsonify(
        {
            "options": unique_values,
            "dates": {"min": date_min, "max": date_max},
            "rows": rows,
        }
    )

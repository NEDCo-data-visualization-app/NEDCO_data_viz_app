"""Filter endpoints for dashboard."""

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

    # Parse selections and facets from payload
    selections = payload.get("selections", {})
    facets_in = payload.get("facets", [])

    # Prepare FilterParams from request
    params = FilterParams(
        start=_parse_date(str(payload.get("start_date") or "")),
        end=_parse_date(str(payload.get("end_date") or "")),
        selections=selections,
        freq=(payload.get("freq") or "D").upper(),
        metric=payload.get("metric") or None,
    )

    date_col = current_app.config["DATE_COL"]

    # Determine table columns dynamically (without loading table)
    # We'll assume the standard facets exist; otherwise rely on config
    resolved_facets = {}
    for candidate in facets_in or ["utility", "tariff_type", "meterid"]:
        resolved_facets[candidate] = candidate  # just use as-is

    # Generate WHERE clause for SQL
    clause, sql_params = params.to_sql_where(
        date_col=date_col,
        available_columns=resolved_facets.values(),  # just use facet names
        param_style="named",
    )

    # Helper to get distinct values for a column directly from BigQuery
    def distinct(col: str) -> List[str]:
        sql = f"""
            SELECT DISTINCT CAST({col} AS STRING) AS v
            FROM `{datastore.TABLE_NAME}`
            WHERE {clause} AND {col} IS NOT NULL
            ORDER BY v
        """
        df = datastore.run_query(sql, sql_params)
        if df is None or df.empty:
            return []
        return df["v"].astype(str).tolist()

    # Fetch unique values for each facet
    unique_values: Dict[str, List[str]] = {
        display_col: distinct(real_col) for display_col, real_col in resolved_facets.items()
    }

    # Apply meterid cap
    meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)
    if "meterid" in unique_values:
        unique_values["meterid"] = unique_values["meterid"][: int(meter_cap)]

    # Fetch min/max dates directly from BigQuery
    sql = f"""
        SELECT
          MIN(CAST({date_col} AS DATE)) AS dmin,
          MAX(CAST({date_col} AS DATE)) AS dmax
        FROM `{datastore.TABLE_NAME}`
        WHERE {clause}
    """
    ddf = datastore.run_query(sql, sql_params)
    date_min = ddf.iloc[0]["dmin"].isoformat() if ddf is not None and ddf.iloc[0]["dmin"] else ""
    date_max = ddf.iloc[0]["dmax"].isoformat() if ddf is not None and ddf.iloc[0]["dmax"] else ""

    # Count rows directly from BigQuery
    sql = f"SELECT COUNT(*) AS n FROM `{datastore.TABLE_NAME}` WHERE {clause};"
    cdf = datastore.run_query(sql, sql_params)
    rows = int(cdf.iloc[0]["n"]) if cdf is not None else 0

    return jsonify(
        {
            "options": unique_values,
            "dates": {"min": date_min, "max": date_max},
            "rows": rows,
        }
    )

"""Meter ID options endpoint (BigQuery-native)."""

from __future__ import annotations

from typing import Dict, List

from flask import current_app, jsonify, request

from . import bp, get_datastore
from .helpers import _parse_date
from volta.utils.filter_params import FilterParams


@bp.route("/options/meterid", methods=["GET", "POST"])
def options_meterid():
    """Return distinct meter IDs respecting filters and search queries."""
    datastore = get_datastore()
    date_col = current_app.config["DATE_COL"]

    # ---------- Determine parameters ----------
    q = ""
    selections: Dict[str, List[str]] = {}
    start_date = end_date = None
    limit = 200

    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        q = str(payload.get("q") or "").strip()
        try:
            limit = max(int(payload.get("limit") or 200), 1)
        except (TypeError, ValueError):
            limit = 200

        raw_selections = payload.get("selections") or {}
        for k, v in raw_selections.items():
            if isinstance(v, (list, tuple)):
                cleaned = [str(x) for x in v if x not in (None, "")]
                if cleaned:
                    selections[k] = cleaned

        start_date = _parse_date(str(payload.get("start_date") or ""))
        end_date = _parse_date(str(payload.get("end_date") or ""))

    else:  # GET
        q = (request.args.get("q") or "").strip()
        try:
            limit = max(int(request.args.get("limit") or 200), 1)
        except (TypeError, ValueError):
            limit = 200
        loc = (request.args.get("utility") or "").strip()
        if loc:
            selections["utility"] = [loc]

        start_date = _parse_date(str(request.args.get("start_date") or ""))
        end_date = _parse_date(str(request.args.get("end_date") or ""))

    # ---------- Build SQL ----------
    sql = f"SELECT DISTINCT CAST(meterid AS STRING) AS v FROM `{datastore.TABLE_NAME}` WHERE meterid IS NOT NULL"
    sql_params: dict[str, object] = {}

    if start_date:
        sql += f" AND {date_col} >= @start_date"
        sql_params["start_date"] = start_date.isoformat() if hasattr(start_date, "isoformat") else str(start_date)
    if end_date:
        sql += f" AND {date_col} <= @end_date"
        sql_params["end_date"] = end_date.isoformat() if hasattr(end_date, "isoformat") else str(end_date)

    for col, values in selections.items():
        if values:
            sql += f" AND {col} IN UNNEST(@{col})"
            sql_params[col] = values

    if q:
        sql += " AND CAST(meterid AS STRING) LIKE CONCAT('%', @q, '%')"
        sql_params["q"] = q

    sql += f" ORDER BY v LIMIT {limit}"

    # ---------- Execute ----------
    df = datastore.run_query(sql, sql_params)
    return jsonify(df["v"].astype(str).tolist() if df is not None else [])

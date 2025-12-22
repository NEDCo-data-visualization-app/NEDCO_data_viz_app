"""Meter ID options endpoint (Datastore-only, no pandas)."""

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
    date_col = current_app.config.get("DATE_COL", "od_date")
    parquet_table = f'"{current_app.config["PARQUET_PATH"]}"'

    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        q = str(payload.get("q") or "").strip()
        try:
            limit = max(int(payload.get("limit") or 200), 1)
        except (TypeError, ValueError):
            limit = 200

        raw_selections = payload.get("selections") or {}

        # Build FilterParams object
        params = FilterParams(
            start=_parse_date(str(payload.get("start_date") or "")),
            end=_parse_date(str(payload.get("end_date") or "")),
            selections=raw_selections,
        )

        clause, sql_params = params.to_sql_where(
            date_col=date_col,
            available_columns=None  # allow filtering on any column
        )

        sql = f'''
            SELECT DISTINCT CAST(meterid AS VARCHAR) AS v
            FROM {parquet_table}
            WHERE {clause}
              AND meterid IS NOT NULL
        '''
        if q:
            sql += " AND CAST(meterid AS VARCHAR) ILIKE '%' || ? || '%'"
            sql_params.append(q)

        sql += " ORDER BY v LIMIT ?"
        sql_params.append(limit)

        rows_gen = datastore.run_query(sql, sql_params, fetch_all=False)
        return jsonify([str(r["v"]) for r in rows_gen])

    # GET request
    q = (request.args.get("q") or "").strip()
    location_param = (request.args.get("utility") or "").strip()
    try:
        limit = max(int(request.args.get("limit") or 200), 1)
    except (TypeError, ValueError):
        limit = 200

    sql = f'''
        SELECT DISTINCT CAST(meterid AS VARCHAR) AS v
        FROM {parquet_table}
        WHERE meterid IS NOT NULL
    '''
    params: List[str] = []

    if location_param:
        sql += " AND CAST(utility AS VARCHAR) = ?"
        params.append(location_param)
    if q:
        sql += " AND CAST(meterid AS VARCHAR) ILIKE '%' || ? || '%'"
        params.append(q)

    sql += " ORDER BY v LIMIT ?"
    params.append(limit)

    rows_gen = datastore.run_query(sql, params, fetch_all=False)
    return jsonify([str(r["v"]) for r in rows_gen])

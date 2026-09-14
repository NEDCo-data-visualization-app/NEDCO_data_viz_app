"""Meter ID search endpoint."""

from __future__ import annotations

from typing import List

from flask import current_app, jsonify, request

from volta.utils.filter_params import FilterParams

from . import bp, get_datastore
from .helpers import _parse_date

DEFAULT_LIMIT = 200


def _limit(raw) -> int:
    try:
        return max(int(raw or DEFAULT_LIMIT), 1)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT


@bp.route("/options/meterid", methods=["GET", "POST"])
def options_meterid():
    """Distinct meter IDs matching a search string, respecting the other filters."""
    datastore = get_datastore()
    columns = datastore.get_columns()
    if "meterid" not in columns:
        return jsonify([])
    date_col = current_app.config.get("DATE_COL", "od_date")

    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        q = str(payload.get("q") or "").strip()
        limit = _limit(payload.get("limit"))
        params = FilterParams(
            start=_parse_date(str(payload.get("start_date") or "")),
            end=_parse_date(str(payload.get("end_date") or "")),
            selections={k: v for k, v in (payload.get("selections") or {}).items() if isinstance(v, list)},
        )
    else:
        q = (request.args.get("q") or "").strip()
        limit = _limit(request.args.get("limit"))
        location = (request.args.get("utility") or "").strip()
        params = FilterParams(selections={"utility": [location]} if location else {})

    clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=columns)
    sql = f"SELECT DISTINCT CAST(meterid AS VARCHAR) AS v FROM {datastore.table_sql} WHERE {clause} AND meterid IS NOT NULL"
    if q:
        sql += " AND CAST(meterid AS VARCHAR) ILIKE '%' || ? || '%'"
        sql_params.append(q)
    sql += " ORDER BY v LIMIT ?"
    sql_params.append(limit)

    rows: List[dict] = datastore.run_query(sql, sql_params)
    return jsonify([str(r["v"]) for r in rows])

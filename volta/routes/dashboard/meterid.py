"""Meter ID options endpoint."""

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

    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        q = str(payload.get("q") or "").strip()
        try:
            limit = int(payload.get("limit") or 200)
        except (TypeError, ValueError):
            limit = 200
        limit = max(limit, 1)

        base = datastore.get(copy=False)
        if base.empty or "meterid" not in base.columns:
            return jsonify([])

        raw_selections = payload.get("selections") or {}
        selections: Dict[str, List[str]] = {}

        cols_lc = {str(c).lower(): c for c in base.columns}
        meterid_real = cols_lc.get("meterid", "meterid")

        for in_key, values in raw_selections.items():
            if not isinstance(values, (list, tuple)):
                continue
            real_col = cols_lc.get(str(in_key).lower())
            if not real_col:
                continue
            if str(real_col).lower() == "meterid":
                continue
            cleaned = [str(v) for v in values if v not in (None, "")]
            if cleaned:
                selections[real_col] = cleaned

        params = FilterParams(
            start=_parse_date(str(payload.get("start_date") or "")),
            end=_parse_date(str(payload.get("end_date") or "")),
            selections=selections,
        )

        date_col = current_app.config["DATE_COL"]
        filtered = params.apply(base, date_col)
        if filtered.empty or meterid_real not in filtered.columns:
            return jsonify([])

        series = filtered[meterid_real].dropna().astype(str)
        if q:
            series = series[series.str.contains(q, case=False, na=False)]

        unique_values = sorted(set(series.tolist()))
        return jsonify(unique_values[:limit])

    q = (request.args.get("q") or "").strip()
    loc = request.args.get("loc")
    try:
        limit = int(request.args.get("limit") or 200)
    except (TypeError, ValueError):
        limit = 200
    limit = max(limit, 1)

    sql = """
        SELECT DISTINCT meterid AS v
        FROM prod.sales
        WHERE meterid IS NOT NULL
    """
    params = []
    if loc:
        sql += " AND CAST(loc AS VARCHAR) = ?"
        params.append(loc)
    if q:
        sql += " AND CAST(meterid AS VARCHAR) ILIKE '%' || ? || '%'"
        params.append(q)

    sql += " ORDER BY v LIMIT ?"
    params.append(limit)

    rows = datastore.run_query(sql, params)
    return jsonify(rows["v"].astype(str).tolist())
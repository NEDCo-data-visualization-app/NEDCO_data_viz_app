"""Time-series data for the line charts (computed entirely in DuckDB)."""

from __future__ import annotations

from flask import current_app, jsonify, request

from . import bp, get_datastore, get_metrics
from .helpers import build_params


@bp.route("/chart-data", methods=["GET"])
def chart_data():
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()
    columns = datastore.get_columns()
    if not columns:
        return jsonify({"labels": [], "values": {}, "metric_labels": {}, "date_col": date_col})

    params = build_params(request.args, base_columns=columns)
    requested = [m for m in (params.metric or "").split(",") if m]
    validated = [m for m in requested if metrics.validate([dict.fromkeys(columns)], m)]
    if not validated:
        return jsonify(
            {
                "labels": [],
                "values": {m: [] for m in requested},
                "metric_labels": {m: metrics.label(m) for m in requested},
                "date_col": date_col,
            }
        )

    clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=columns)
    agg = "SUM" if (request.args.get("agg") or "mean").lower() in ("sum", "total") else "AVG"
    trunc_unit = params.trunc_unit()
    label_format = "%Y-%m" if trunc_unit == "month" else "%Y-%m-%d"
    metric_sql = ", ".join(f"COALESCE({agg}({m}), 0) AS {m}" for m in validated)

    sql = f"""
        SELECT strftime(date_trunc('{trunc_unit}', {date_col}), '{label_format}') AS bucket, {metric_sql}
        FROM {datastore.table_sql}
        WHERE {clause}
        GROUP BY 1 ORDER BY 1
    """
    rows = datastore.run_query(sql, sql_params)

    return jsonify(
        {
            "labels": [r["bucket"] for r in rows],
            "values": {m: [r[m] for r in rows] for m in validated},
            "metric_labels": {m: metrics.label(m) for m in validated},
            "date_col": date_col,
        }
    )

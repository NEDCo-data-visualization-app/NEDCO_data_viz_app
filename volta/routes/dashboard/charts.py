from __future__ import annotations
from flask import current_app, jsonify, request
from . import bp, get_datastore, get_metrics
from .helpers import build_params

@bp.route("/chart-data", methods=["GET"])
def chart_data():
    """Time-series for charts: fully computed in DuckDB, minimal Python processing."""
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()
    columns = datastore.get_columns()
    if not columns:
        return jsonify({"labels": [], "values": {}, "metric_labels": {}, "date_col": date_col})

    params = build_params(request.args, base_columns=columns)
    requested_metrics = [m for m in (params.metric or "").split(",") if m]

    sample_row = next(datastore.run_query(f'SELECT * FROM "{current_app.config["PARQUET_PATH"]}" LIMIT 1', fetch_all=False), {})
    validated_metrics = [m for m in requested_metrics if metrics.validate([sample_row], m)]
    if not validated_metrics:
        return jsonify({
            "labels": [],
            "values": {m: [] for m in requested_metrics},
            "metric_labels": {m: metrics.label(m) for m in requested_metrics},
            "date_col": date_col,
        })

    clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=columns)
    agg_func = request.args.get("agg", "mean").lower()
    sql_agg = "SUM" if agg_func in ("sum", "total") else "AVG"
    trunc_unit = params.trunc_unit()
    label_format = "%Y-%m" if trunc_unit == "month" else "%Y-%m-%d"

    metric_sql = ", ".join(f"COALESCE({sql_agg}({m}),0) AS {m}" for m in validated_metrics)
    parquet_table = f'"{current_app.config["PARQUET_PATH"]}"'

    sql = f"""
        SELECT strftime('{label_format}', date_trunc('{trunc_unit}', {date_col})) AS bucket,
               {metric_sql}
        FROM {parquet_table}
    """
    if clause:
        sql += f" WHERE {clause}"
    sql += " GROUP BY 1 ORDER BY 1"

    rows = datastore.run_query(sql, sql_params, fetch_all=False)

    labels, values = [], {m: [] for m in validated_metrics}
    for r in rows:
        labels.append(r["bucket"])
        for m in validated_metrics:
            values[m].append(r[m])

    return jsonify({
        "labels": labels,
        "values": values,
        "metric_labels": {m: metrics.label(m) for m in validated_metrics},
        "date_col": date_col,
    })

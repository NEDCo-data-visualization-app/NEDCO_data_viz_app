"""Aggregate chart endpoints (DuckDB + Parquet, no pandas)."""

from __future__ import annotations

from flask import current_app, jsonify, request

from . import bp, get_datastore, get_metrics
from .helpers import build_params


@bp.route("/pie-data", methods=["GET"])
def pie_data():
    """Pie chart data endpoint."""
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()

    # Build params WITHOUT loading any data
    params = build_params(request.args, None)
    metric = params.metric

    # Use cached column names to validate metric
    columns = datastore.get_columns()
    fake_row = dict.fromkeys(columns)  # single row for validation

    if not metrics.validate([fake_row], metric):
        return jsonify({"labels": [], "values": [], "metric_label": "", "segment": ""})

    # Determine segmentation column (probe schema)
    segment_col = None
    segment_alias = ""
    for col, alias in [("tariff_type", "res_mapped"), ("utility", "loc")]:
        if col in columns:
            segment_col = col
            segment_alias = alias
            break

    if not segment_col:
        return jsonify({"labels": [], "values": [], "metric_label": "", "segment": ""})

    clause, sql_params = params.to_sql_where(
        date_col=date_col,
        available_columns=[segment_col, metric]
    )

    sql = f"""
        SELECT
            CAST({segment_col} AS VARCHAR) AS label,
            SUM({metric}) AS value
        FROM '{current_app.config["PARQUET_PATH"]}'
        WHERE {clause} AND {segment_col} IS NOT NULL
        GROUP BY 1
        ORDER BY value DESC
    """

    rows = datastore.run_query(sql, sql_params)
    if not rows:
        return jsonify({
            "labels": [],
            "values": [],
            "metric_label": metrics.label(metric),
            "segment": segment_alias,
        })

    # ---------- Top-N logic ----------
    top_n = 8
    if len(rows) > top_n:
        top = rows[:top_n]
        other_val = sum(float(r["value"]) for r in rows[top_n:])
        labels = [r["label"] for r in top] + ["Other"]
        values = [float(r["value"]) for r in top] + [other_val]
    else:
        labels = [r["label"] for r in rows]
        values = [float(r["value"]) for r in rows]

    return jsonify({
        "labels": labels,
        "values": values,
        "metric_label": metrics.label(metric),
        "segment": segment_alias,
    })


@bp.route("/bar-data", methods=["GET"])
def bar_data():
    """Bar chart data endpoint."""
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()

    params = build_params(request.args, None)
    metric = params.metric

    # Validate metric against cached columns
    columns = datastore.get_columns()
    fake_row = dict.fromkeys(columns)
    if not metrics.validate([fake_row], metric):
        return jsonify({
            "labels": [],
            "values": [],
            "metric_label": metrics.label(metric),
            "segment": "utility"
        })

    clause, sql_params = params.to_sql_where(
        date_col=date_col,
        available_columns=["utility", metric]
    )

    sql = f"""
        SELECT
            CAST(utility AS VARCHAR) AS label,
            SUM({metric}) AS value
        FROM '{current_app.config["PARQUET_PATH"]}'
        WHERE {clause} AND utility IS NOT NULL
        GROUP BY 1
        ORDER BY value DESC
    """

    rows = datastore.run_query(sql, sql_params)
    if not rows:
        return jsonify({
            "labels": [],
            "values": [],
            "metric_label": metrics.label(metric),
            "segment": "utility"
        })

    labels = [r["label"] for r in rows]
    values = [float(r["value"]) for r in rows]

    return jsonify({
        "labels": labels,
        "values": values,
        "metric_label": metrics.label(metric),
        "segment": "utility"
    })

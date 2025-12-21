"""Aggregate chart endpoints (DuckDB + Parquet, full FilterParams integration)."""

from __future__ import annotations

from flask import current_app, jsonify, request

from . import bp, get_datastore, get_metrics
from .helpers import build_params


def _aggregate_query(segment_col: str, metric: str, params, top_n: int = 8):
    """
    Shared helper to run aggregation on a segment column and metric
    while applying FilterParams.
    """
    datastore = get_datastore()
    date_col = current_app.config["DATE_COL"]

    # Get all columns for validation
    columns = datastore.get_columns()
    clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=columns)

    sql = f'''
        SELECT
            CAST({segment_col} AS VARCHAR) AS label,
            SUM({metric}) AS value
        FROM "{current_app.config["PARQUET_PATH"]}"
        WHERE {clause} AND {segment_col} IS NOT NULL
        GROUP BY 1
        ORDER BY value DESC
    '''
    rows = datastore.run_query(sql, sql_params)

    if not rows:
        return [], []

    # Apply top-N logic for pie charts
    if len(rows) > top_n:
        top = rows[:top_n]
        other_val = sum(float(r["value"]) for r in rows[top_n:])
        labels = [r["label"] for r in top] + ["Other"]
        values = [float(r["value"]) for r in top] + [other_val]
    else:
        labels = [r["label"] for r in rows]
        values = [float(r["value"]) for r in rows]

    return labels, values


@bp.route("/pie-data", methods=["GET"])
def pie_data():
    """
    Pie chart endpoint that respects all dashboard filters (FilterParams).
    """
    datastore = get_datastore()
    metrics = get_metrics()
    columns = datastore.get_columns()
    # Build FilterParams from request args
    params = build_params(request.args, base_columns=columns)
    metric = params.metric

    # Validate metric against all columns
    fake_row = dict.fromkeys(columns)
    if not metrics.validate([fake_row], metric):
        return jsonify({"labels": [], "values": [], "metric_label": "", "segment": ""})

    # Choose segmentation column
    for col, alias in [("tariff_type", "res_mapped"), ("utility", "loc")]:
        if col in columns:
            segment_col = col
            segment_alias = alias
            break
    else:
        return jsonify({"labels": [], "values": [], "metric_label": "", "segment": ""})

    labels, values = _aggregate_query(segment_col, metric, params)

    return jsonify({
        "labels": labels,
        "values": values,
        "metric_label": metrics.label(metric),
        "segment": segment_alias,
    })


@bp.route("/bar-data", methods=["GET"])
def bar_data():
    """
    Bar chart endpoint that respects all dashboard filters (FilterParams).
    """
    datastore = get_datastore()
    metrics = get_metrics()
    columns = datastore.get_columns()
    params = build_params(request.args, base_columns=columns)
    metric = params.metric

    fake_row = dict.fromkeys(columns)
    if not metrics.validate([fake_row], metric):
        return jsonify({"labels": [], "values": [], "metric_label": metrics.label(metric), "segment": "utility"})

    segment_col = "utility"
    labels, values = _aggregate_query(segment_col, metric, params, top_n=float("inf"))

    return jsonify({
        "labels": labels,
        "values": values,
        "metric_label": metrics.label(metric),
        "segment": segment_col,
    })

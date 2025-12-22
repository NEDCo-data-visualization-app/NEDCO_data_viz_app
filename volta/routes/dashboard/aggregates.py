from __future__ import annotations
from flask import current_app, jsonify, request
from . import bp, get_datastore, get_metrics
from .helpers import build_params

def _aggregate_sql(segment_col: str, metric: str, params, top_n: int = 8):
    """Build SQL to aggregate a metric by segment_col with filters applied, handles top-N + 'Other' in SQL."""
    datastore = get_datastore()
    date_col = current_app.config["DATE_COL"]
    columns = datastore.get_columns()
    clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=columns)
    parquet_table = f'"{current_app.config["PARQUET_PATH"]}"'

    sql_base = f"""
        SELECT
            CAST({segment_col} AS VARCHAR) AS label,
            SUM({metric}) AS value
        FROM {parquet_table}
        WHERE {clause} AND {segment_col} IS NOT NULL
        GROUP BY 1
        ORDER BY value DESC
    """

    if top_n and top_n > 0:
        sql = f"""
            WITH ranked AS ({sql_base}),
                 top_rows AS (SELECT *, ROW_NUMBER() OVER (ORDER BY value DESC) AS rn FROM ranked),
                 top_n AS (SELECT label, value FROM top_rows WHERE rn <= {top_n}),
                 other AS (SELECT 'Other' AS label, SUM(value) AS value FROM top_rows WHERE rn > {top_n})
            SELECT label, value FROM top_n
            UNION ALL
            SELECT label, value FROM other WHERE value IS NOT NULL
        """
    else:
        sql = sql_base

    return sql, sql_params

@bp.route("/pie-data", methods=["GET"])
def pie_data():
    datastore = get_datastore()
    metrics = get_metrics()
    columns = datastore.get_columns()
    params = build_params(request.args, base_columns=columns)
    metric = params.metric

    # Validate metric
    fake_row = dict.fromkeys(columns)
    if not metrics.validate([fake_row], metric):
        return jsonify({"labels": [], "values": [], "metric_label": "", "segment": ""})

    # Determine segment column
    for col, alias in [("tariff_type", "res_mapped"), ("utility", "loc")]:
        if col in columns:
            segment_col, segment_alias = col, alias
            break
    else:
        return jsonify({"labels": [], "values": [], "metric_label": "", "segment": ""})

    sql, sql_params = _aggregate_sql(segment_col, metric, params, top_n=8)
    rows = datastore.run_query(sql, sql_params, fetch_all=False)  # streaming generator

    labels, values = [], []
    for r in rows:
        labels.append(r["label"])
        values.append(float(r["value"]))

    return jsonify({
        "labels": labels,
        "values": values,
        "metric_label": metrics.label(metric),
        "segment": segment_alias,
    })

@bp.route("/bar-data", methods=["GET"])
def bar_data():
    datastore = get_datastore()
    metrics = get_metrics()
    columns = datastore.get_columns()
    params = build_params(request.args, base_columns=columns)

    # Get all requested metrics from query string (comma-separated)
    metric_list = [m for m in request.args.get("metric", "").split(",") if m]
    if not metric_list:
        return jsonify([])

    segment_col = "utility"
    series_list = []

    for metric in metric_list:
        # Validate metric
        fake_row = dict.fromkeys(columns)
        if not metrics.validate([fake_row], metric):
            continue

        # Aggregate SQL
        sql, sql_params = _aggregate_sql(segment_col, metric, params, top_n=None)
        rows = datastore.run_query(sql, sql_params, fetch_all=False)  # streaming

        labels, values = [], []
        for r in rows:
            labels.append(r["label"])
            values.append(float(r["value"]))

        if labels and values:
            series_list.append({
                "labels": labels,
                "values": values,
                "metric_label": metrics.label(metric),
                "segment": segment_col
            })

    return jsonify(series_list)

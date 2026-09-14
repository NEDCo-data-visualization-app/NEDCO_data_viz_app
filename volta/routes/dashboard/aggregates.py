"""Composition (donut) and by-city (bar) aggregates."""

from __future__ import annotations

from flask import current_app, jsonify, request

from . import bp, get_datastore, get_metrics
from .helpers import build_params


def _aggregate_sql(segment_col: str, metric: str, params, top_n: int | None = 8):
    """SUM(metric) grouped by segment_col with filters applied; optional top-N + 'Other'."""
    datastore = get_datastore()
    date_col = current_app.config["DATE_COL"]
    clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=datastore.get_columns())

    sql_base = f"""
        SELECT CAST({segment_col} AS VARCHAR) AS label, SUM({metric}) AS value
        FROM {datastore.table_sql}
        WHERE {clause} AND {segment_col} IS NOT NULL
        GROUP BY 1
        ORDER BY value DESC
    """
    if top_n and top_n > 0:
        sql = f"""
            WITH ranked AS ({sql_base}),
                 numbered AS (SELECT *, ROW_NUMBER() OVER (ORDER BY value DESC) AS rn FROM ranked)
            SELECT label, value FROM numbered WHERE rn <= {int(top_n)}
            UNION ALL
            SELECT 'Other' AS label, SUM(value) AS value FROM numbered WHERE rn > {int(top_n)} HAVING SUM(value) IS NOT NULL
        """
    else:
        sql = sql_base
    return sql, sql_params


def _series(rows):
    labels, values = [], []
    for r in rows:
        labels.append(r["label"])
        values.append(float(r["value"] or 0))
    return labels, values


@bp.route("/pie-data", methods=["GET"])
def pie_data():
    datastore = get_datastore()
    metrics = get_metrics()
    columns = datastore.get_columns()
    params = build_params(request.args, base_columns=columns)
    metric = params.metric
    empty = {"labels": [], "values": [], "metric_label": "", "segment": ""}

    if not metrics.validate([dict.fromkeys(columns)], metric):
        return jsonify(empty)

    for col, alias in (("tariff_type", "res_mapped"), ("utility", "loc")):
        if col in columns:
            segment_col, segment_alias = col, alias
            break
    else:
        return jsonify(empty)

    sql, sql_params = _aggregate_sql(segment_col, metric, params, top_n=8)
    labels, values = _series(datastore.run_query(sql, sql_params))
    return jsonify({"labels": labels, "values": values, "metric_label": metrics.label(metric), "segment": segment_alias})


@bp.route("/bar-data", methods=["GET"])
def bar_data():
    datastore = get_datastore()
    metrics = get_metrics()
    columns = datastore.get_columns()
    params = build_params(request.args, base_columns=columns)

    metric_list = [m for m in (params.metric or "").split(",") if m]
    if not metric_list or "utility" not in columns:
        return jsonify([])

    series_list = []
    for metric in metric_list:
        if not metrics.validate([dict.fromkeys(columns)], metric):
            continue
        sql, sql_params = _aggregate_sql("utility", metric, params, top_n=None)
        labels, values = _series(datastore.run_query(sql, sql_params))
        if labels:
            series_list.append({"labels": labels, "values": values, "metric_label": metrics.label(metric), "segment": "utility"})
    return jsonify(series_list)

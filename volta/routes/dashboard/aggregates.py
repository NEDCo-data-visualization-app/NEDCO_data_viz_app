"""Aggregate chart endpoints."""

from __future__ import annotations

import pandas as pd
from flask import current_app, jsonify, request

from . import bp, get_datastore, get_metrics
from .helpers import build_params


@bp.route("/pie-data", methods=["GET"])
def pie_data():
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()

    # Build params from request args
    base_df = current_app.config.get("BASE_DF", pd.DataFrame())
    params = build_params(request.args, base_df)

    # Validate metric
    metric = metrics.validate(base_df, params.metric)
    if not metric:
        return jsonify({"labels": [], "values": [], "metric_label": params.metric or "", "segment": ""})

    # Determine segment
    if "tariff_type" in base_df.columns:
        segment_col = "tariff_type"
        segment_alias = "res_mapped"
    elif "utility" in base_df.columns:
        segment_col = "utility"
        segment_alias = "loc"
    else:
        segment_col = None
        segment_alias = ""

    if segment_col is None:
        return jsonify({"labels": [], "values": [], "metric_label": metrics.label(metric), "segment": segment_alias})

    # Build BigQuery SQL
    sql = f"""
        SELECT {segment_col}, SUM({metric}) AS metric_sum
        FROM `{datastore.TABLE_NAME}`
        GROUP BY {segment_col}
    """

    # Execute query
    df = datastore.run_query(sql)

    if df is None or df.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metrics.label(metric), "segment": segment_alias})

    # Process series
    series = df.dropna(subset=[segment_col, "metric_sum"]).copy()
    series["metric_sum"] = pd.to_numeric(series["metric_sum"], errors="coerce")
    series = series.dropna(subset=["metric_sum"])
    if series.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metrics.label(metric), "segment": segment_alias})

    grp = series.groupby(series[segment_col].astype(str))["metric_sum"].sum().sort_values(ascending=False)

    # Top N logic
    top_n = 8
    if len(grp) > top_n:
        top = grp.iloc[:top_n]
        other_val = float(grp.iloc[top_n:].sum())
        labels = top.index.tolist() + ["Other"]
        values = [float(v) for v in top.values] + [other_val]
    else:
        labels = grp.index.tolist()
        values = [float(v) for v in grp.values]

    return jsonify({"labels": labels, "values": values, "metric_label": metrics.label(metric), "segment": segment_alias})


@bp.route("/bar-data", methods=["GET"])
def bar_data():
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()

    base_df = current_app.config.get("BASE_DF", pd.DataFrame())
    params = build_params(request.args, base_df)

    metric = metrics.validate(base_df, params.metric)
    city_col = "utility"
    city_alias = "utility"

    if not metric:
        return jsonify({"labels": [], "values": [], "metric_label": params.metric or "", "segment": city_alias})

    sql = f"""
        SELECT {city_col}, SUM({metric}) AS metric_sum
        FROM `{datastore.TABLE_NAME}`
        GROUP BY {city_col}
    """

    df = datastore.run_query(sql)

    if df is None or df.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metrics.label(metric), "segment": city_alias})

    series = df.dropna(subset=[city_col, "metric_sum"]).copy()
    series["metric_sum"] = pd.to_numeric(series["metric_sum"], errors="coerce")
    series = series.dropna(subset=["metric_sum"])
    if series.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metrics.label(metric), "segment": city_alias})

    grp = series.groupby(series[city_col].astype(str))["metric_sum"].sum().sort_values(ascending=False)
    labels = grp.index.tolist()
    values = [float(v) for v in grp.values]

    return jsonify({"labels": labels, "values": values, "metric_label": metrics.label(metric), "segment": city_alias})

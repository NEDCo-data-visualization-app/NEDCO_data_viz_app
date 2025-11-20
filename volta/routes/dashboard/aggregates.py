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
    base = datastore.get(copy=False)
    params = build_params(request.args, base)
    filtered = params.apply(base, date_col)

    metric = metrics.validate(filtered, params.metric)
    if "tariff_type" in filtered.columns:
        segment_col = "tariff_type"
        segment_alias = "res_mapped"
    elif "utility" in filtered.columns:
        segment_col = "utility"
        segment_alias = "loc"
    else:
        segment_col = None
        segment_alias = ""

    if not metric or segment_col is None or filtered.empty:
        return jsonify(
            {
                "labels": [],
                "values": [],
                "metric_label": params.metric or "",
                "segment": segment_alias,
            }
        )

    series = filtered.dropna(subset=[segment_col]).copy()
    series[metric] = pd.to_numeric(series[metric], errors="coerce")
    series = series.dropna(subset=[metric])
    if series.empty:
        return jsonify(
            {
                "labels": [],
                "values": [],
                "metric_label": metrics.label(metric),
                "segment": segment_alias,
            }
        )

    grp = series.groupby(series[segment_col].astype(str))[metric].sum().sort_values(
        ascending=False
    )

    top_n = 8
    if len(grp) > top_n:
        top = grp.iloc[:top_n]
        other_val = float(grp.iloc[top_n:].sum())
        labels = top.index.tolist() + ["Other"]
        values = [float(v) for v in top.values] + [other_val]
    else:
        labels = grp.index.tolist()
        values = [float(v) for v in grp.values]

    return jsonify(
        {
            "labels": labels,
            "values": values,
            "metric_label": metrics.label(metric),
            "segment": segment_alias,
        }
    )


@bp.route("/bar-data", methods=["GET"])
def bar_data():
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()
    base = datastore.get(copy=False)
    params = build_params(request.args, base)
    filtered = params.apply(base, date_col)

    metric = metrics.validate(filtered, params.metric)
    city_col = "utility"
    city_alias = "utility"

    if not metric or city_col not in filtered.columns or filtered.empty:
        return jsonify(
            {
                "labels": [],
                "values": [],
                "metric_label": params.metric or "",
                "segment": city_alias,
            }
        )

    series = filtered.dropna(subset=[city_col]).copy()
    series[metric] = pd.to_numeric(series[metric], errors="coerce")
    series = series.dropna(subset=[metric])
    if series.empty:
        return jsonify(
            {
                "labels": [],
                "values": [],
                "metric_label": metrics.label(metric),
                "segment": city_col,
            }
        )

    grp = series.groupby(series[city_col].astype(str))[metric].sum().sort_values(
        ascending=False
    )

    labels = grp.index.tolist()
    values = [float(v) for v in grp.values]
    return jsonify(
        {
            "labels": labels,
            "values": values,
            "metric_label": metrics.label(metric),
            "segment": city_alias,
        }
    )
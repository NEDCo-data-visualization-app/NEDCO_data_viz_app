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
    segment_col = (
        "res_mapped"
        if "res_mapped" in filtered.columns
        else ("loc" if "loc" in filtered.columns else None)
    )

    if not metric or segment_col is None or filtered.empty:
        return jsonify(
            {
                "labels": [],
                "values": [],
                "metric_label": params.metric or "",
                "segment": segment_col or "",
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
                "segment": segment_col,
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
            "segment": segment_col,
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
    city_col = "loc"

    if not metric or city_col not in filtered.columns or filtered.empty:
        return jsonify(
            {
                "labels": [],
                "values": [],
                "metric_label": params.metric or "",
                "segment": city_col or "",
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
            "segment": city_col,
        }
    )
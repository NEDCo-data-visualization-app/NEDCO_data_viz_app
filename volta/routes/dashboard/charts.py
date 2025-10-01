"""Chart data endpoints."""

from __future__ import annotations

import pandas as pd
from flask import current_app, jsonify, request

from . import bp, get_datastore, get_metrics
from .helpers import build_params


@bp.route("/chart-data", methods=["GET"])
def chart_data():
    """Time-series for charts, computed in DuckDB (fast)."""
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()

    base = datastore.get(copy=False)
    params = build_params(request.args, base)

    requested_metrics = (params.metric or "").split(",")
    validated_metrics = [m for m in requested_metrics if metrics.validate(base, m)]

    if not validated_metrics:
        return jsonify(
            {"labels": [], "values": [], "metric_label": params.metric or "", "date_col": date_col}
        )

    clause, sql_params = params.to_sql_where(
        date_col=date_col,
        available_columns=base.columns,
    )

    trunc_unit = params.trunc_unit()

    metric_sql = ", ".join([f"AVG({m}) AS {m}" for m in validated_metrics])
    sql = f"""
        SELECT
          date_trunc('{trunc_unit}', {date_col}) AS bucket,
          {metric_sql}
        FROM prod.sales
        WHERE {clause}
        GROUP BY 1
        ORDER BY 1;
    """

    df = datastore.run_query(sql, sql_params)

    if df is None or df.empty:
        return jsonify(
            {
                "labels": [],
                "values": {m: [] for m in validated_metrics},
                "metric_labels": {m: metrics.label(m) for m in validated_metrics},
                "date_col": date_col,
            }
        )

    def _fmt(ts: pd.Timestamp) -> str:
        if params.freq == "M":
            return pd.to_datetime(ts).strftime("%Y-%m")
        return pd.to_datetime(ts).date().isoformat()

    labels = [_fmt(v) for v in df["bucket"]]

    values_dict = {}
    for m in validated_metrics:
        values_dict[m] = [float(v) if pd.notna(v) else 0.0 for v in df[m]]

    return jsonify(
        {
            "labels": labels,
            "values": values_dict,
            "metric_labels": {m: metrics.label(m) for m in validated_metrics},
            "date_col": date_col,
        }
    )
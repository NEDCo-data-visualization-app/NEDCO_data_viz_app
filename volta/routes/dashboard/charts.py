"""Chart data endpoints."""

from __future__ import annotations

import pandas as pd
from flask import current_app, jsonify, request

from . import bp, get_datastore, get_metrics
from .helpers import build_params


@bp.route("/chart-data", methods=["GET"])
def chart_data():
    """Time-series for charts, computed directly on BigQuery."""

    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()
    base_df = current_app.config.get("BASE_DF", pd.DataFrame())

    # Parse request args
    params = build_params(request.args, base_df)

    # Validate metrics
    requested_metrics = (params.metric or "").split(",")
    validated_metrics = [m for m in requested_metrics if metrics.validate(base_df, m)]

    if not validated_metrics:
        return jsonify(
            {"labels": [], "values": {}, "metric_labels": {}, "date_col": date_col}
        )

    # Build WHERE clause: only start/end dates
    sql_clauses = [f"{date_col} BETWEEN @date_from AND @date_to"]

    # Convert dates to ISO format to avoid JSON serialization errors
    sql_params = {
        "date_from": params.start.isoformat() if hasattr(params.start, "isoformat") else params.start,
        "date_to": params.end.isoformat() if hasattr(params.end, "isoformat") else params.end,
    }

    where_clause = " AND ".join(sql_clauses)

    # Time truncation
    trunc_unit = params.trunc_unit()  # 'D', 'M', etc.
    trunc_sql = {"D": "DAY", "M": "MONTH", "Y": "YEAR"}.get(trunc_unit.upper(), "DAY")

    # Aggregation
    agg_func = request.args.get("agg", "mean").lower()
    sql_agg = "SUM" if agg_func in ("sum", "total") else "AVG"

    metric_sql = ", ".join([f"{sql_agg}({m}) AS {m}" for m in validated_metrics])

    sql = f"""
        SELECT
            DATE_TRUNC({date_col}, {trunc_sql}) AS bucket,
            {metric_sql}
        FROM `{datastore.TABLE_NAME}`
        WHERE {date_col} BETWEEN DATE('{sql_params["date_from"]}') AND DATE('{sql_params["date_to"]}')
        GROUP BY bucket
        ORDER BY bucket
    """

    # Execute query
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

    # Format labels safely for JSON
    def _fmt(v) -> str:
        # Handle pandas Timestamp
        if isinstance(v, pd.Timestamp):
            return v.strftime("%Y-%m") if params.freq.upper() == "M" else v.date().isoformat()
        # Handle Python date
        if hasattr(v, "isoformat"):
            return v.strftime("%Y-%m") if params.freq.upper() == "M" else v.isoformat()
        # Fallback
        return str(v)

    labels = [_fmt(v) for v in df["bucket"]]
    values_dict = {m: [float(v) if pd.notna(v) else 0.0 for v in df[m]] for m in validated_metrics}

    return jsonify(
        {
            "labels": labels,
            "values": values_dict,
            "metric_labels": {m: metrics.label(m) for m in validated_metrics},
            "date_col": date_col,
        }
    )

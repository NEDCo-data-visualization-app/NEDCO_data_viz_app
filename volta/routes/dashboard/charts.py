"""Chart data endpoints (DuckDB + Parquet, no pandas)."""

from __future__ import annotations

from flask import current_app, jsonify, request

from . import bp, get_datastore, get_metrics
from .helpers import build_params

@bp.route("/chart-data", methods=["GET"])
def chart_data():
    """Time-series for charts, computed fully in DuckDB."""
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()

    # Build FilterParams from request args
    params = build_params(request.args, None)

    # --- Use cached columns for validation ---
    columns = datastore.get_columns()
    fake_row = dict.fromkeys(columns)  # single dict to represent a row
    requested_metrics = [m for m in (params.metric or "").split(",") if m]
    validated_metrics = [m for m in requested_metrics if metrics.validate([fake_row], m)]

    if not validated_metrics:
        return jsonify(
            {
                "labels": [],
                "values": {m: [] for m in requested_metrics},
                "metric_labels": {m: metrics.label(m) for m in requested_metrics},
                "date_col": date_col,
            }
        )

    clause, sql_params = params.to_sql_where(
        date_col=date_col,
        available_columns=[date_col] + validated_metrics,
    )

    trunc_unit = params.trunc_unit()
    agg_func = request.args.get("agg", "mean").lower()
    sql_agg = "SUM" if agg_func in ("sum", "total") else "AVG"

    metric_sql = ", ".join(f"{sql_agg}({m}) AS {m}" for m in validated_metrics)

    sql = f"""
        SELECT
            date_trunc('{trunc_unit}', {date_col}) AS bucket,
            {metric_sql}
        FROM '{current_app.config["PARQUET_PATH"]}'
        WHERE {clause}
        GROUP BY 1
        ORDER BY 1
    """
    rows = datastore.run_query(sql, sql_params)
    if not rows:
        return jsonify(
            {
                "labels": [],
                "values": {m: [] for m in validated_metrics},
                "metric_labels": {m: metrics.label(m) for m in validated_metrics},
                "date_col": date_col,
            }
        )

    # ---------- Format labels ----------
    if params.freq == "M":
        labels = [str(r["bucket"])[:7] for r in rows]  # YYYY-MM
    else:
        labels = [str(r["bucket"])[:10] for r in rows]  # YYYY-MM-DD

    # ---------- Build values ----------
    values: dict[str, list[float]] = {
        m: [float(r[m]) if r[m] is not None else 0.0 for r in rows]
        for m in validated_metrics
    }

    return jsonify(
        {
            "labels": labels,
            "values": values,
            "metric_labels": {m: metrics.label(m) for m in validated_metrics},
            "date_col": date_col,
        }
    )

"""Chart data endpoints (DuckDB + Parquet, no pandas) with debug logging."""

from __future__ import annotations
from flask import current_app, jsonify, request
from . import bp, get_datastore, get_metrics
from .helpers import build_params

@bp.route("/chart-data", methods=["GET"])
def chart_data():
    """Time-series for charts, computed fully in DuckDB with debug logging."""
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()

    # Step 1: fetch all columns from DuckDB (full table)
    columns = datastore.get_columns()
    if not columns:
        return jsonify({
            "labels": [],
            "values": {},
            "metric_labels": {},
            "date_col": date_col,
        })

    # Step 2: build FilterParams from request args using all columns
    params = build_params(request.args, base_columns=columns)

    # Step 3: validate requested metrics using first real row
    requested_metrics = [m for m in (params.metric or "").split(",") if m]
    sample_rows = datastore.run_query(f'SELECT * FROM "{current_app.config["PARQUET_PATH"]}" LIMIT 1')
    first_row = sample_rows[0] if sample_rows else dict.fromkeys(columns)
    validated_metrics = [m for m in requested_metrics if metrics.validate([first_row], m)]

    if not validated_metrics:
        return jsonify({
            "labels": [],
            "values": {m: [] for m in requested_metrics},
            "metric_labels": {m: metrics.label(m) for m in requested_metrics},
            "date_col": date_col,
        })

    # Step 4: generate SQL WHERE clause using all columns
    clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=columns)

    trunc_unit = params.trunc_unit()
    agg_func = request.args.get("agg", "mean").lower()
    sql_agg = "SUM" if agg_func in ("sum", "total") else "AVG"
    metric_sql = ", ".join(f"{sql_agg}({m}) AS {m}" for m in validated_metrics)

    parquet_table = f'"{current_app.config["PARQUET_PATH"]}"'

    sql = f"""
        SELECT
            date_trunc('{trunc_unit}', {date_col}) AS bucket,
            {metric_sql}
        FROM {parquet_table}
    """
    if clause:
        sql += f" WHERE {clause}"
    sql += " GROUP BY 1 ORDER BY 1"

    # Step 5: fetch rows from DuckDB
    rows = datastore.run_query(sql, sql_params)

    if not rows:
        return jsonify({
            "labels": [],
            "values": {m: [] for m in validated_metrics},
            "metric_labels": {m: metrics.label(m) for m in validated_metrics},
            "date_col": date_col,
        })

    # Step 6: format labels
    if params.freq == "M":
        labels = [str(r["bucket"])[:7] for r in rows]  # YYYY-MM
    else:
        labels = [str(r["bucket"])[:10] for r in rows]  # YYYY-MM-DD

    # Step 7: build values
    values: dict[str, list[float]] = {
        m: [float(r[m]) if r[m] is not None else 0.0 for r in rows]
        for m in validated_metrics
    }

    return jsonify({
        "labels": labels,
        "values": values,
        "metric_labels": {m: metrics.label(m) for m in validated_metrics},
        "date_col": date_col,
    })

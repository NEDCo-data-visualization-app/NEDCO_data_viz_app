"""Dashboard index view (fully datastore-based, no pandas)."""

from __future__ import annotations
import logging
from flask import (
    current_app,
    jsonify,
    make_response,
    render_template,
    request,
)
from markupsafe import escape
from . import bp, get_datastore, get_metrics, get_predictor
from .helpers import DEFAULT_METERID_LIMIT, build_params, build_unique_values
from volta.utils.filter_params import FilterParams
import tracemalloc
tracemalloc.start()
PREDICT_ALL_AS_OF = "09-2020"
PREVIEW_ROW_LIMIT = 10
METRIC_COLUMNS = ["ocd_energy", "ocd_cash_received", "ocd_paymoney"]

LEGACY_PREDICTION_OUTPUT = {
    "kwh": "ocd_energy",
    "ghc": "ocd_cash_received",
    "paymoney": "ocd_paymoney",
}

COLUMN_LABEL_DEFAULTS = {
    "meterid": "Meter ID",
    "forecast_date": "Forecast Month",
    "horizon": "Horizon (Months Ahead)",
    "ocd_energy": "Energy (kWh)",
    "ocd_cash_received": "Cash Received (GHC)",
    "ocd_paymoney": "Paymoney",
}

COLUMN_CLASS_MAP = {
    "meterid": "text-nowrap text-center",
    "forecast_date": "text-nowrap",
    "horizon": "text-nowrap text-center",
    "ocd_energy": "text-end",
    "ocd_cash_received": "text-end",
    "ocd_paymoney": "text-nowrap text-end",
}


def _format_prediction_value(column: str, value: object) -> str:
    """Format a prediction value for display."""
    if value is None:
        return "—"
    try:
        if column == "forecast_date":
            return str(value)
        elif column == "horizon":
            return str(int(float(value)))
        elif column in METRIC_COLUMNS:
            return f"{float(value):,.2f}"
        else:
            return str(escape(str(value)))
    except Exception:
        return str(escape(str(value)))


def _render_prediction_preview_table(rows: list[dict[str, object]], limit: int = PREVIEW_ROW_LIMIT) -> str:
    """Render HTML table preview from list-of-dicts."""
    if not rows or limit <= 0:
        return ""

    rows = rows[:limit]
    columns = list(rows[0].keys())
    metrics_service = get_metrics()
    column_labels = dict(COLUMN_LABEL_DEFAULTS)

    if metrics_service and isinstance(getattr(metrics_service, "mapping", None), dict):
        column_labels.update(metrics_service.mapping)

    header_cells = [
        f'<th scope="col" class="{COLUMN_CLASS_MAP.get(col, "text-nowrap")}">{escape(column_labels.get(col, col))}</th>'
        for col in columns
    ]

    body_rows = []
    for row in rows:
        cells = [
            f'<td class="{COLUMN_CLASS_MAP.get(col, "text-nowrap")}">{_format_prediction_value(col, row.get(col))}</td>'
            for col in columns
        ]
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    table_html = (
        '<table class="table table-sm table-striped table-hover align-middle mb-0">'
        f"<thead class=\"table-light\"><tr>{''.join(header_cells)}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )
    return table_html


def _collect_historical_monthly(as_of: str, meterid: str | None = None) -> list[dict[str, object]]:
    """Aggregate historical monthly metrics via datastore."""
    datastore = get_datastore()
    date_col = current_app.config.get("DATE_COL", "od_date")

    clauses = [f"{date_col} IS NOT NULL", f"{date_col} <= ?"]
    params: list[object] = [as_of]

    if meterid:
        clauses.append("CAST(meterid AS VARCHAR) = ?")
        params.append(str(meterid))

    where_sql = " AND ".join(clauses)
    sql = f'''
        SELECT
            CAST(date_trunc('month', {date_col}) AS TEXT) AS month,
            SUM(ocd_energy) AS ocd_energy,
            SUM(ocd_cash_received) AS ocd_cash_received,
            SUM(ocd_paymoney) AS ocd_paymoney
        FROM "{current_app.config["PARQUET_PATH"]}"
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY 1
    '''
    try:
        return datastore.run_query(sql, params)
    except Exception:
        current_app.logger.exception("Historical monthly aggregation failed")
        return []


def _collect_forecast_monthly(predictions: list[dict[str, object]]) -> list[dict[str, object]]:
    """Aggregate forecast monthly metrics."""
    if not predictions:
        return []

    monthly_map: dict[str, dict[str, object]] = {}
    for row in predictions:
        month = row.get("forecast_date")
        if month is None:
            continue
        metrics = {metric: row.get(metric) for metric in METRIC_COLUMNS}
        if month not in monthly_map:
            monthly_map[month] = {"month": month, **metrics}
        else:
            for metric, value in metrics.items():
                monthly_map[month][metric] = (monthly_map[month].get(metric, 0) or 0) + (value or 0)

    return list(monthly_map.values())


def _convert_to_legacy_metrics(records: list[dict[str, object]]) -> list[dict[str, object]]:
    """Map internal metric column names to legacy API keys."""
    converted = []
    for row in records:
        item = {"month": row.get("month")}
        for legacy, internal in LEGACY_PREDICTION_OUTPUT.items():
            item[legacy] = row.get(internal)
        converted.append(item)
    return converted


def index():
    datastore = get_datastore()
    metrics = get_metrics()
    date_col = current_app.config.get("DATE_COL", "od_date")

    # Step 1: get columns from DuckDB
    columns = datastore.get_columns()
    print("Columns loaded", tracemalloc.get_traced_memory())
    if not columns:
        return render_template("upload.html")

    # Step 2: build FilterParams from request.args
    params = build_params(request.args, base_columns=columns)

    # Step 3: construct WHERE clause from FilterParams
    clause, sql_params = (
        params.to_sql_where(date_col=date_col, available_columns=columns)
        if (params.selections or params.start or params.end)
        else ("", [])
    )

    # Step 4: fetch preview rows only (memory-safe)
    preview_rows_sql = f'SELECT * FROM "{current_app.config["PARQUET_PATH"]}"'
    if clause:
        preview_rows_sql += f" WHERE {clause}"
    preview_rows_sql += f" LIMIT {PREVIEW_ROW_LIMIT}"

    preview_rows = datastore.run_query(preview_rows_sql, sql_params)
    print("Preview rows loaded", tracemalloc.get_traced_memory())

    # Step 5: compute stats / summary in SQL (no memory load)
    stats = datastore.compute_stats(where_clause=clause, sql_params=sql_params)
    print("Stats computed", tracemalloc.get_traced_memory())
    summary = datastore.compute_summary(where_clause=clause, sql_params=sql_params)
    print("Summary computed", tracemalloc.get_traced_memory())

    # Step 6: start / end date from summary
    start_value = summary.get("date_min", "")
    end_value = summary.get("date_max", "")

    # Step 7: unique meter & utility values with limits
    meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)
    unique_values = build_unique_values(datastore, ["meterid", "utility"], clause, sql_params, max_uniques=meter_cap)

    # Step 8: chart metrics (based on preview rows)
    chart_metrics = metrics.available(preview_rows)
    default_metric = chart_metrics[0][0] if chart_metrics else ""

    # Step 9: render preview table
    preview_html = _render_prediction_preview_table(preview_rows, PREVIEW_ROW_LIMIT)

    # Step 10: render template
    return render_template(
        "index.html",
        date_col=date_col,
        stats=stats,
        summary=summary,
        start_value=start_value,
        end_value=end_value,
        unique_values=unique_values,
        args=request.args,
        total_rows=summary.get("rows", 0),  
        total_cols=len(columns),
        preview_html=preview_html,
        chart_metrics=chart_metrics,
        default_metric=default_metric,
    )


def predictions():
    datastore = get_datastore()
    meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)
    sql = f'SELECT DISTINCT meterid AS v FROM "{current_app.config["PARQUET_PATH"]}" WHERE meterid IS NOT NULL ORDER BY v LIMIT {meter_cap}'
    rows_gen = datastore.run_query(sql, fetch_all=False)
    meter_options = [str(r["v"]) for r in rows_gen]

    return render_template(
        "predictions.html",
        meter_options=meter_options,
        meterid_limit=int(meter_cap),
    )


def _run_predict_all():
    datastore = get_datastore()
    predictor = get_predictor()
    return predictor.predict_all_from_db(datastore=datastore)


def _run_predict_one(meterid: int):
    datastore = get_datastore()
    predictor = get_predictor()
    return predictor.predict_one_meter_from_db(datastore=datastore, meterid=meterid)


def predictions_predict_all():
    try:
        predictions_list = _run_predict_all()
    except Exception:
        current_app.logger.exception("Predict All request failed")
        return jsonify({"ok": False, "error": "Unable to generate predictions at this time."}), 500

    as_of = PREDICT_ALL_AS_OF
    if hasattr(predictions_list, "to_dict"):
        predictions_list = [
            {k: v for k, v in row.items()} for row in predictions_list.to_dict(orient="records")
        ]

    forecast_monthly = _convert_to_legacy_metrics(_collect_forecast_monthly(predictions_list))
    historical_monthly = _convert_to_legacy_metrics(_collect_historical_monthly(as_of))

    row_count = len(predictions_list)
    preview_html = _render_prediction_preview_table(predictions_list, min(PREVIEW_ROW_LIMIT, row_count))

    return jsonify({
        "ok": True,
        "row_count": row_count,
        "preview_rows": min(PREVIEW_ROW_LIMIT, row_count),
        "preview_html": preview_html,
        "as_of": as_of,
        "scope": "all",
        "charts": {
            "historical": historical_monthly,
            "forecast": forecast_monthly,
        },
    })


def predictions_predict_one():
    payload = request.get_json(silent=True) or {}
    meterid_raw = str(payload.get("meterid", "")).strip()
    if not meterid_raw:
        return jsonify({"ok": False, "error": "Select a meter before running Predict."}), 200

    try:
        meterid_int = int(meterid_raw)
    except ValueError:
        return jsonify({"ok": False, "error": "Meter ID must be a valid number."}), 200

    predictions_list = _run_predict_one(meterid_int)
    as_of = PREDICT_ALL_AS_OF

    if hasattr(predictions_list, "to_dict"):
        predictions_list = [
            {k: v for k, v in row.items()} for row in predictions_list.to_dict(orient="records")
        ]

    forecast_monthly = _convert_to_legacy_metrics(_collect_forecast_monthly(predictions_list))
    historical_monthly = _convert_to_legacy_metrics(_collect_historical_monthly(as_of, meterid=meterid_raw))

    row_count = len(predictions_list)
    preview_html = _render_prediction_preview_table(predictions_list, min(PREVIEW_ROW_LIMIT, row_count))

    return jsonify({
        "ok": True,
        "row_count": row_count,
        "preview_rows": min(PREVIEW_ROW_LIMIT, row_count),
        "preview_html": preview_html,
        "as_of": as_of,
        "meterid": meterid_raw,
        "scope": "meter",
        "charts": {
            "historical": historical_monthly,
            "forecast": forecast_monthly,
        },
    })


def predictions_download():
    meterid_raw = str(request.args.get("meterid", "")).strip()
    datastore = get_datastore()

    if meterid_raw:
        try:
            meterid_int = int(meterid_raw)
        except (TypeError, ValueError):
            return make_response("Meter ID must be a valid number", 400)

        as_of = PREDICT_ALL_AS_OF
        predictions_list = _run_predict_one(meterid_int)
        filename = f"predict_meter_{meterid_raw}.csv"

    else:
        as_of = PREDICT_ALL_AS_OF
        predictions_list = _run_predict_all()
        filename = "predict_all.csv"

    # Convert internal metric names to legacy keys
    if predictions_list:
        for row in predictions_list:
            for legacy, internal in LEGACY_PREDICTION_OUTPUT.items():
                row[legacy] = row.pop(internal, None)

    # Convert to CSV
    import csv
    from io import StringIO

    output = StringIO()
    if predictions_list:
        writer = csv.DictWriter(output, fieldnames=predictions_list[0].keys())
        writer.writeheader()
        writer.writerows(predictions_list)
    csv_content = output.getvalue()
    output.close()

    response = make_response(csv_content)
    response.headers["Content-Type"] = "text/csv"
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


# Register routes
bp.add_url_rule("/", view_func=index, methods=["GET"])
bp.add_url_rule("/predictions", view_func=predictions, methods=["GET"])
bp.add_url_rule("/predictions/download", view_func=predictions_download, methods=["GET"])
bp.add_url_rule("/predictions/api/predict-all", view_func=predictions_predict_all, methods=["POST"])
bp.add_url_rule("/predictions/api/predict", view_func=predictions_predict_one, methods=["POST"])

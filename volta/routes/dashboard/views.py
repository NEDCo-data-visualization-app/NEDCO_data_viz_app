"""Dashboard index view (fully datastore-based, no pandas)."""

from __future__ import annotations
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
import tracemalloc
import json
import datetime
from werkzeug.datastructures import ImmutableMultiDict
tracemalloc.start()
PREDICT_ALL_AS_OF = "2020-09-01"
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

MODE = True

def cache_dashboard_first_load(datastore, preview_rows, stats, summary, unique_values, chart_metrics, preview_html):
    # Convert all date objects to strings in preview_rows
    def convert_dates(obj):
        if isinstance(obj, list):
            return [convert_dates(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: convert_dates(v) for k, v in obj.items()}
        elif isinstance(obj, (datetime.date, datetime.datetime)):
            return str(obj)
        else:
            return obj

    preview_rows_json = json.dumps(convert_dates(preview_rows))
    stats_json = json.dumps(convert_dates(stats))
    summary_json = json.dumps(convert_dates(summary))
    unique_values_json = json.dumps(convert_dates(unique_values))
    chart_metrics_json = json.dumps(chart_metrics)

    datastore._con.execute("""
        CREATE TABLE IF NOT EXISTS dashboard_cache (
            cache_key VARCHAR PRIMARY KEY,
            preview_rows   JSON,
            stats          JSON,
            summary        JSON,
            unique_values  JSON,
            chart_metrics  JSON,
            preview_html   VARCHAR
        );
    """)

    datastore._con.execute("""
        INSERT INTO dashboard_cache (cache_key, preview_rows, stats, summary, unique_values, chart_metrics, preview_html)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(cache_key) DO UPDATE SET
            preview_rows = excluded.preview_rows,
            stats = excluded.stats,
            summary = excluded.summary,
            unique_values = excluded.unique_values,
            chart_metrics = excluded.chart_metrics,
            preview_html = excluded.preview_html
    """, [
        "full_load",
        preview_rows_json,
        stats_json,
        summary_json,
        unique_values_json,
        chart_metrics_json,
        preview_html
    ])



    
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


def _render_prediction_preview_table(
    rows: list[dict[str, object]], 
    limit: int = PREVIEW_ROW_LIMIT, 
    is_public: bool = MODE
) -> str:
    """Render HTML table preview from list-of-dicts, hiding sensitive columns in public mode."""
    
    if not rows or limit <= 0:
        return ""

    rows = rows[:limit]

    # Determine which columns to show
    columns = list(rows[0].keys())
    if is_public:
        # hide sensitive columns
        columns = [c for c in columns if c.lower() not in ("meterid", "customer_no")]

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
            CAST(date_trunc('month', {date_col}) AS DATE) AS month,
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
    """
    Aggregate forecast metrics by month-end, ensuring output compatible with legacy charts.
    Input: list of dicts (predictions)
    Output: list of dicts with keys: month, ocd_energy, ocd_cash_received, ocd_paymoney
    """
    if not predictions:
        return []

    monthly_map: dict[str, dict[str, object]] = {}

    for i, row in enumerate(predictions):
        # Get the forecast date
        month_val = row.get("Forecast Month") or row.get("forecast_date") or row.get("prediction_date")
        if not month_val:
            continue

        # Convert to datetime
        if isinstance(month_val, str):
            try:
                month_dt = datetime.datetime.fromisoformat(month_val)
            except ValueError:
                continue
        elif isinstance(month_val, datetime.date):
            month_dt = datetime.datetime.combine(month_val, datetime.time.min)
        elif isinstance(month_val, datetime.datetime):
            month_dt = month_val
        else:
            continue

        # Normalize to month-end
        next_month = (month_dt.replace(day=1) + datetime.timedelta(days=32)).replace(day=1)
        month_end = next_month - datetime.timedelta(days=1)
        month_key = month_end.date().isoformat()

        # Extract metrics with fallback keys
        metrics = {
            "ocd_energy": row.get("Energy (kWh)") or row.get("ocd_energy") or row.get("energy_pred") or 0,
            "ocd_paymoney": row.get("Paymoney") or row.get("ocd_paymoney") or row.get("paymoney_pred") or 0,
            "ocd_cash_received": row.get("Cash Received (GHC)") or row.get("ocd_cash_received") or row.get("cash_pred") or 0,
        }

        # Aggregate sums per month
        if month_key not in monthly_map:
            monthly_map[month_key] = {"month": month_key, **metrics}
        else:
            for k, v in metrics.items():
                monthly_map[month_key][k] = (monthly_map[month_key].get(k, 0) or 0) + (v or 0)

    # Sort by month
    result = sorted(monthly_map.values(), key=lambda x: x["month"])
    return result




def _convert_to_legacy_metrics(records: list[dict[str, object]]) -> list[dict[str, object]]:
    """Map internal metric column names to legacy API keys and format month ISO."""
    converted = []
    for row in records:
        month_val = row.get("month")
        if isinstance(month_val, (datetime.date, datetime.datetime)):
            month_val = month_val.strftime("%Y-%m-%d")
        item = {"month": str(month_val)}
        for legacy, internal in LEGACY_PREDICTION_OUTPUT.items():
            val = row.get(internal)
            item[legacy] = float(val) if val is not None else 0
        converted.append(item)
    return converted

def index(first_load_override: bool | None = None, is_public: bool = MODE):
    """
    Dashboard index view. If first_load_override=True, treat as first load (ignore request.args).
    """
    datastore = get_datastore()
    metrics = get_metrics()
    date_col = current_app.config.get("DATE_COL", "od_date")

    # Step 1: get columns from DuckDB
    columns = datastore.get_columns()
    print("Columns loaded", tracemalloc.get_traced_memory())
    if not columns:
        return render_template("upload.html")

    # Step 2: build FilterParams from request.args or override
    args_to_use = ImmutableMultiDict() if first_load_override else request.args
    params = build_params(args_to_use, base_columns=columns)

    # Step 3: construct WHERE clause from FilterParams
    clause, sql_params = (
        params.to_sql_where(date_col=date_col, available_columns=columns)
        if (params.selections or params.start or params.end)
        else ("", [])
    )

    # Step 4: determine if this is the "first load"
    first_load = first_load_override if first_load_override is not None else not request.args

    preview_rows = []
    stats = {}
    summary = {}
    unique_values = {}
    chart_metrics = []
    preview_html = ""
    row = None

    # --- Step 4a: ensure cache table exists ---
    datastore._con.execute("""
        CREATE TABLE IF NOT EXISTS dashboard_cache (
            cache_key VARCHAR PRIMARY KEY,
            preview_rows   JSON,
            stats          JSON,
            summary        JSON,
            unique_values  JSON,
            chart_metrics  JSON,
            preview_html   VARCHAR
        );
    """)
# --- Step 5: fetch preview rows only ---
    preview_rows_sql = f'SELECT * FROM "{current_app.config["PARQUET_PATH"]}"'
    if clause:
        preview_rows_sql += f" WHERE {clause}"
    preview_rows_sql += f" LIMIT {PREVIEW_ROW_LIMIT}"

    preview_rows = datastore.run_query(preview_rows_sql, sql_params)

    # --- Step 4b (modified): if first load, cache dashboard now that preview_rows exists ---
    if first_load:
        row = datastore._con.execute(
            "SELECT * FROM dashboard_cache WHERE cache_key='full_load'"
        ).fetchone()

        meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)
        unique_values = build_unique_values(
            datastore, ["meterid", "utility", "tariff_type"], clause, sql_params, max_uniques=meter_cap
        )
        chart_metrics = metrics.available(preview_rows)
        preview_html = _render_prediction_preview_table(preview_rows, PREVIEW_ROW_LIMIT, is_public=is_public)

        cache_dashboard_first_load(
            datastore,
            preview_rows,
            stats,
            summary,
            unique_values,
            chart_metrics,
            preview_html
        )

    # --- Step 5: fetch preview rows only ---
    preview_rows_sql = f'SELECT * FROM "{current_app.config["PARQUET_PATH"]}"'
    if clause:
        preview_rows_sql += f" WHERE {clause}"
    preview_rows_sql += f" LIMIT {PREVIEW_ROW_LIMIT}"

    preview_rows = datastore.run_query(preview_rows_sql, sql_params)
    print("Preview rows loaded", tracemalloc.get_traced_memory())

    # --- Step 6: compute stats / summary ---
    stats = datastore.compute_stats(where_clause=clause, sql_params=sql_params)
    summary = datastore.compute_summary(where_clause=clause, sql_params=sql_params)
    print("Stats and summary computed", tracemalloc.get_traced_memory())

    # Step 7: start / end date from summary
    start_value = summary.get("date_min", "")
    end_value = summary.get("date_max", "")

    # Step 8: unique meter & utility values with limits
    meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)

    # Only include meterid if not public
    cols_to_include = ["utility", "tariff_type"] if is_public else ["meterid", "utility", "tariff_type"]

    unique_values = build_unique_values(
        datastore, cols_to_include, clause, sql_params, max_uniques=meter_cap
    )

    # Step 9: chart metrics (based on preview rows)
    chart_metrics = metrics.available(preview_rows)
    default_metric = chart_metrics[0][0] if chart_metrics else ""

    # Step 10: render preview table
    preview_html = _render_prediction_preview_table(preview_rows, PREVIEW_ROW_LIMIT)

    # Step 11: cache first-load results if not already cached
    if first_load and not row:
        cache_dashboard_first_load(
            datastore,
            preview_rows,
            stats,
            summary,
            unique_values,
            chart_metrics,
            preview_html
        )
        print("Dashboard first-load cached", tracemalloc.get_traced_memory())

    # Step 12: render template
    return render_template(
        "index.html",
        date_col=date_col,
        stats=stats,
        summary=summary,
        start_value=start_value,
        end_value=end_value,
        unique_values=unique_values,
        args=args_to_use,
        total_rows=summary.get("rows", 0),
        total_cols=len(columns),
        preview_html=preview_html,
        chart_metrics=chart_metrics,
        default_metric=default_metric,
        is_public=is_public,
    )

def predictions(is_public:bool = MODE):
    datastore = get_datastore()
    meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)
    sql = f'SELECT DISTINCT meterid AS v FROM "{current_app.config["PARQUET_PATH"]}" WHERE meterid IS NOT NULL ORDER BY v LIMIT {meter_cap}'
    rows_gen = datastore.run_query(sql, fetch_all=False)
    meter_options = [str(r["v"]) for r in rows_gen]

    return render_template(
        "predictions.html",
        meter_options=meter_options,
        meterid_limit=int(meter_cap),
        is_public = is_public
    )


def _run_predict_all():
    predictor = get_predictor()
    return predictor.predict_all_from_db(history_months=24)


def _run_predict_one(meterid: int):
    predictor = get_predictor()
    return predictor.predict_one_meter_from_db(meterid)


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
bp.add_url_rule(
    "/public-dashboard",
    view_func=lambda: index(is_public=True),
    methods=["GET"],
    endpoint="public_dashboard"  
)
bp.add_url_rule(
    "/reset-dashboard",
    view_func=lambda: index(first_load_override=True),
    methods=["POST"],
    endpoint="reset_dashboard"  
)
# Private predictions (meter filter visible)
bp.add_url_rule(
    "/predictions/private",
    view_func=lambda: predictions(is_public=False),
    methods=["GET"],
    endpoint="predictions_private"
)

# Public predictions (no meter filter)
bp.add_url_rule(
    "/predictions",
    view_func=lambda: predictions(is_public=True),
    methods=["GET"],
    endpoint="predictions_public"
)

bp.add_url_rule("/predictions/download", view_func=predictions_download, methods=["GET"])
bp.add_url_rule("/predictions/api/predict-all", view_func=predictions_predict_all, methods=["POST"])
bp.add_url_rule("/predictions/api/predict", view_func=predictions_predict_one, methods=["POST"])
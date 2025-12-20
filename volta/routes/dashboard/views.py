"""Dashboard index view with BigQuery support (BigQuery-native)."""

from __future__ import annotations

import pandas as pd
from flask import (
    current_app,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    url_for,
)
from markupsafe import escape
from . import bp, get_datastore, get_metrics, get_predictor
from .helpers import DEFAULT_METERID_LIMIT, build_params, build_unique_values, no_filters_selected

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
    "ocd_energy": "Energy (kWh)",
    "ocd_cash_received": "Cash Received (GHC)",
    "ocd_paymoney": "text-nowrap text-end",
}


def _format_prediction_value(column: str, value: object) -> str:
    if value is None:
        return "—"
    try:
        if pd.isna(value):
            return "—"
    except TypeError:
        pass
    if column == "forecast_date":
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return str(escape(str(value)))
        return parsed.date().isoformat()
    if column == "horizon":
        try:
            return f"{int(float(value))}"
        except (TypeError, ValueError):
            return str(escape(str(value)))
    if column in METRIC_COLUMNS:
        try:
            return f"{float(value):,.2f}"
        except (TypeError, ValueError):
            return str(escape(str(value)))
    return str(escape(str(value)))


def _render_prediction_preview_table(df: pd.DataFrame, limit: int = PREVIEW_ROW_LIMIT) -> str:
    if df is None or df.empty or limit <= 0:
        return ""
    metrics_service = get_metrics()
    column_labels = dict(COLUMN_LABEL_DEFAULTS)
    if metrics_service:
        service_mapping = getattr(metrics_service, "mapping", None)
        if isinstance(service_mapping, dict):
            column_labels.update(service_mapping)
        else:
            try:
                column_labels.update(dict(metrics_service))  # type: ignore[arg-type]
            except TypeError:
                pass
    priority_columns = ["meterid", "forecast_date", "horizon", *METRIC_COLUMNS]
    columns = [col for col in priority_columns if col in df.columns]
    columns.extend(col for col in df.columns if col not in columns)
    if not columns:
        return ""
    preview = df.loc[:, columns].head(limit)
    header_cells = [
        f'<th scope="col" class="{COLUMN_CLASS_MAP.get(col, "text-nowrap")}">'
        f"{str(escape(str(column_labels.get(col, col.replace('_', ' ').title()))))}</th>"
        for col in columns
    ]
    body_rows = []
    for row in preview.itertuples(index=False, name=None):
        cells = [
            f'<td class="{COLUMN_CLASS_MAP.get(col, "text-nowrap")}">{_format_prediction_value(col, val)}</td>'
            for col, val in zip(columns, row)
        ]
        body_rows.append(f"<tr>{''.join(cells)}</tr>")
    table_html = (
        '<table class="table table-sm table-striped table-hover align-middle mb-0">'
        f"<thead class='table-light'><tr>{''.join(header_cells)}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )
    return table_html


# ------------------------ Index route (BigQuery-native) ------------------------

def index():
    datastore = get_datastore()
    metrics = get_metrics()
    date_col = current_app.config.get("DATE_COL", "od_date")
    meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)

    # Fetch a few rows to serve as the base DataFrame for filtering
    try:
        preview_sql = f"SELECT * FROM `{datastore.TABLE_NAME}` LIMIT 1"
        base_df = datastore.run_query(preview_sql)
    except Exception:
        base_df = pd.DataFrame()  # fallback if query fails

    current_app.config["BASE_DF"] = base_df

    # Redirect if no filters selected
    if request.args and not base_df.empty and no_filters_selected(request.args, base_df):
        return redirect(url_for("dashboard.index"))

    # Build WHERE clauses from request args
    params = build_params(request.args, base_df)
    where_clause, sql_params = params.to_sql_where(date_col=date_col)

    # Fetch preview rows
    preview_sql = f"""
        SELECT *
        FROM `{datastore.TABLE_NAME}`
        WHERE {where_clause}
        ORDER BY {date_col} DESC
        LIMIT {PREVIEW_ROW_LIMIT}
    """
    try:
        preview_df = datastore.run_query(preview_sql, sql_params)
        preview_html = preview_df.to_html(
            classes="table table-sm table-striped table-hover", index=False
        )
    except Exception:
        preview_html = ""

    # Fetch distinct meterid options
    meterids = []
    try:
        meter_sql = f"""
            SELECT DISTINCT CAST(meterid AS STRING) AS v
            FROM `{datastore.TABLE_NAME}`
            WHERE meterid IS NOT NULL
            ORDER BY v
            LIMIT {int(meter_cap)}
        """
        meterids = datastore.run_query(meter_sql)["v"].astype(str).tolist()
    except Exception:
        meterids = []

    # Fetch distinct utility options
    utilities = []
    try:
        util_sql = f"""
            SELECT DISTINCT CAST(utility AS STRING) AS v
            FROM `{datastore.TABLE_NAME}`
            WHERE utility IS NOT NULL
            ORDER BY v
        """
        utilities = datastore.run_query(util_sql)["v"].astype(str).tolist()
    except Exception:
        utilities = []

    # Determine start and end dates
    start_value = end_value = ""
    try:
        date_sql = f"""
            SELECT MIN({date_col}) AS start_date, MAX({date_col}) AS end_date
            FROM `{datastore.TABLE_NAME}`
        """
        dates = datastore.run_query(date_sql)
        if not dates.empty:
            start_value = pd.to_datetime(dates.at[0, "start_date"], errors="coerce").date().isoformat() \
                if "start_date" in dates.columns else ""
            end_value = pd.to_datetime(dates.at[0, "end_date"], errors="coerce").date().isoformat() \
                if "end_date" in dates.columns else ""
    except Exception:
        pass

    # Compute stats
    try:
        stats = datastore.compute_stats(params.start, params.end)
    except Exception:
        stats = pd.DataFrame()

    # Compute chart metrics using the Metrics service
    chart_metrics = metrics.available(base_df) if metrics and not base_df.empty else []
    default_metric = chart_metrics[0][0] if chart_metrics else ""

    return render_template(
        "index.html",
        date_col=date_col,
        stats=stats,
        stats_exists=not stats.empty if stats is not None else False,
        summary=None,  # If you have a summary method, replace None
        start_value=start_value,
        end_value=end_value,
        unique_values={"meterid": meterids, "utility": utilities},
        args=request.args,
        total_rows=None,  # If you have a count_rows method, replace None
        total_cols=len(base_df.columns) if not base_df.empty else 0,
        preview_html=preview_html,
        chart_metrics=chart_metrics,
        default_metric=default_metric,
    )



# ------------------------ Predictions (same as before) ------------------------

def predictions():
    datastore = get_datastore()
    meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)
    meter_options = []

    # Fetch distinct meterid options
    try:
        meter_sql = f"""
            SELECT DISTINCT CAST(meterid AS STRING) AS v
            FROM `{datastore.TABLE_NAME}`
            WHERE meterid IS NOT NULL
            ORDER BY v
            LIMIT {int(meter_cap)}
        """
        meter_options = datastore.run_query(meter_sql)["v"].astype(str).tolist()
    except Exception:
        meter_options = []

    return render_template("predictions.html", meter_options=meter_options, meterid_limit=int(meter_cap))


def _run_predict_all():
    predictor = get_predictor()
    return _prepare_predictions_df(predictor.predict_all_from_db())


def _run_predict_one(meterid: int):
    predictor = get_predictor()
    return _prepare_predictions_df(predictor.predict_one_meter_from_db(meterid=meterid))


def predictions_predict_all():
    try:
        predictions_df = _run_predict_all()
    except Exception:
        current_app.logger.exception("Predict All request failed")
        return jsonify({"ok": False, "error": "Unable to generate predictions at this time."}), 500
    if predictions_df is None:
        predictions_df = pd.DataFrame()
    as_of = _extract_as_of(predictions_df) or _get_dataset_last_date()
    forecast_monthly = _convert_to_legacy_metrics(_collect_forecast_monthly(predictions_df))
    historical_monthly = _convert_to_legacy_metrics(_collect_historical_monthly(as_of)) if forecast_monthly else []
    row_count = len(predictions_df)
    preview_rows = min(PREVIEW_ROW_LIMIT, row_count)
    preview_html = _render_prediction_preview_table(predictions_df, preview_rows) if row_count else ""
    return jsonify({"ok": True, "row_count": row_count, "preview_rows": preview_rows, "preview_html": preview_html,
                    "as_of": as_of, "scope": "all", "charts": {"historical": historical_monthly, "forecast": forecast_monthly}})


def predictions_predict_one():
    payload = request.get_json(silent=True) or {}
    meterid_raw = str(payload.get("meterid", "")).strip()
    if not meterid_raw:
        return jsonify({"ok": False, "error": "Select a meter before running Predict."}), 200
    try:
        meterid_int = int(meterid_raw)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Meter ID must be a valid number."}), 200
    as_of = _get_meter_last_date(meterid_raw)
    if not as_of:
        return jsonify({"ok": False, "error": "No historical data found for the selected meter."}), 200
    try:
        predictions_df = _run_predict_one(meterid_int)
    except Exception:
        current_app.logger.exception("Predict meter request failed")
        return jsonify({"ok": False, "error": "Unable to generate predictions at this time."}), 200
    if predictions_df is None:
        predictions_df = pd.DataFrame()
    inferred_as_of = _extract_as_of(predictions_df)
    if inferred_as_of:
        as_of = inferred_as_of
    forecast_monthly = _convert_to_legacy_metrics(_collect_forecast_monthly(predictions_df))
    historical_monthly = _convert_to_legacy_metrics(_collect_historical_monthly(as_of, meterid=meterid_raw)) if forecast_monthly else []
    row_count = len(predictions_df)
    preview_rows = min(PREVIEW_ROW_LIMIT, row_count)
    preview_html = _render_prediction_preview_table(predictions_df, preview_rows) if row_count else ""
    return jsonify({"ok": True, "row_count": row_count, "preview_rows": preview_rows, "preview_html": preview_html,
                    "as_of": as_of, "meterid": meterid_raw, "scope": "meter",
                    "charts": {"historical": historical_monthly, "forecast": forecast_monthly}})


def predictions_download():
    meterid_raw = str(request.args.get("meterid", "")).strip()
    if meterid_raw:
        try:
            meterid_int = int(meterid_raw)
        except (TypeError, ValueError):
            return make_response("Meter ID must be a valid number", 400)
        as_of = _get_meter_last_date(meterid_raw)
        if not as_of:
            return make_response("No historical data found for the selected meter", 404)
        try:
            predictions_df = _run_predict_one(meterid_int)
        except Exception:
            current_app.logger.exception("Predict meter download failed")
            return make_response("Unable to generate predictions", 500)
        if predictions_df is None:
            predictions_df = pd.DataFrame()
        filename = f"predict_meter_{meterid_raw}.csv"
    else:
        as_of = PREDICT_ALL_AS_OF
        try:
            predictions_df = _run_predict_all()
        except Exception:
            current_app.logger.exception("Predict All download failed")
            return make_response("Unable to generate predictions", 500)
        if predictions_df is None:
            predictions_df = pd.DataFrame()
        filename = "predict_all.csv"
    if not predictions_df.empty:
        rename_map = {new: legacy for legacy, new in LEGACY_PREDICTION_OUTPUT.items()}
        predictions_df = predictions_df.rename(columns=rename_map)
    csv_content = predictions_df.to_csv(index=False)
    response = make_response(csv_content)
    response.headers["Content-Type"] = "text/csv"
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response


# ------------------------ URL rules ------------------------

bp.add_url_rule("/", view_func=index, methods=["GET"])
bp.add_url_rule("/predictions", view_func=predictions, methods=["GET"])
bp.add_url_rule("/predictions/api/predict-all", view_func=predictions_predict_all, methods=["POST"])
bp.add_url_rule("/predictions/api/predict", view_func=predictions_predict_one, methods=["POST"])
bp.add_url_rule("/predictions/download", view_func=predictions_download, methods=["GET"])

"""Dashboard index view."""

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


def _render_prediction_preview_table(
    df: pd.DataFrame, limit: int = PREVIEW_ROW_LIMIT
) -> str:
    if df is None or df.empty or limit <= 0:
        return ""

    metrics_service = get_metrics()
    column_labels = dict(COLUMN_LABEL_DEFAULTS)

    if metrics_service:
        # ``Metrics`` exposes a ``mapping`` attribute for label lookups.  Fall back to
        # treating the object as a mapping if an alternative implementation is
        # supplied via configuration.
        service_mapping = getattr(metrics_service, "mapping", None)
        if isinstance(service_mapping, dict):
            column_labels.update(service_mapping)
        else:
            try:
                column_labels.update(dict(metrics_service))  # type: ignore[arg-type]
            except TypeError:
                pass

    priority_columns: list[str] = [
        "meterid",
        "forecast_date",
        "horizon",
        *METRIC_COLUMNS,
    ]
    columns: list[str] = [col for col in priority_columns if col in df.columns]
    columns.extend(col for col in df.columns if col not in columns)

    if not columns:
        return ""

    preview = df.loc[:, columns].head(limit)

    header_cells: list[str] = []
    for column in columns:
        label = column_labels.get(column, column.replace("_", " ").title())
        classes = COLUMN_CLASS_MAP.get(column, "text-nowrap")
        header_cells.append(
            f'<th scope="col" class="{classes}">{str(escape(str(label)))}</th>'
        )

    body_rows: list[str] = []
    for row in preview.itertuples(index=False, name=None):
        cells: list[str] = []
        for column, value in zip(columns, row):
            cell_classes = COLUMN_CLASS_MAP.get(column, "text-nowrap")
            display_value = _format_prediction_value(column, value)
            cells.append(f'<td class="{cell_classes}">{display_value}</td>')
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    table_html = (
        '<table class="table table-sm table-striped table-hover align-middle mb-0">'
        f"<thead class=\"table-light\"><tr>{''.join(header_cells)}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )

    return table_html


def _normalize_month_end(as_of: str) -> pd.Timestamp | None:
    """Return the month-end Timestamp for an ``as_of`` string."""

    if not as_of:
        return None

    parsed = pd.to_datetime(as_of, format="%m-%Y", errors="coerce")
    if pd.isna(parsed):
        parsed = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(parsed):
        return None
    return (parsed + pd.offsets.MonthEnd(0)).normalize()


def _serialize_monthly(df: pd.DataFrame, date_column: str) -> list[dict[str, object]]:
    if df is None or df.empty or date_column not in df.columns:
        return []

    rows: list[dict[str, object]] = []
    for _, row in df.iterrows():
        month = pd.to_datetime(row.get(date_column), errors="coerce")
        if pd.isna(month):
            continue
        item: dict[str, object] = {"month": month.date().isoformat()}
        for metric in METRIC_COLUMNS:
            value = row.get(metric)
            if pd.isna(value):
                item[metric] = None
            elif value is None:
                item[metric] = None
            else:
                try:
                    item[metric] = float(value)
                except (TypeError, ValueError):
                    item[metric] = None
        rows.append(item)
    return rows

def _convert_to_legacy_metrics(records: list[dict[str, object]]) -> list[dict[str, object]]:
    """Map internal metric column names to legacy API keys for predictions charts."""

    converted: list[dict[str, object]] = []
    for row in records:
        item: dict[str, object] = {}
        if "month" in row:
            item["month"] = row["month"]
        for legacy_key, new_key in LEGACY_PREDICTION_OUTPUT.items():
            item[legacy_key] = row.get(new_key)
        converted.append(item)
    return converted

def _prepare_predictions_df(predictions_df: pd.DataFrame | None) -> pd.DataFrame:
    """Rename predictor outputs to the columns expected by the UI."""

    if predictions_df is None:
        return pd.DataFrame()

    rename_map = {
        "prediction_date": "forecast_date",
        "paymoney_pred": "ocd_paymoney",
        "energy_pred": "ocd_energy",
        "cash_pred": "ocd_cash_received",
    }

    df = predictions_df.rename(columns=rename_map)

    if "forecast_date" in df.columns:
        df["forecast_date"] = pd.to_datetime(df["forecast_date"], errors="coerce")

    return df


def _extract_as_of(predictions_df: pd.DataFrame) -> str | None:
    """Return the latest as_of date from the predictions frame if present."""

    if "as_of" not in predictions_df.columns or predictions_df.empty:
        return None

    ts = pd.to_datetime(predictions_df["as_of"], errors="coerce")
    ts = ts.dropna()
    if ts.empty:
        return None

    return ts.max().date().isoformat()





def _convert_to_legacy_metrics(records: list[dict[str, object]]) -> list[dict[str, object]]:
    """Map internal metric column names to legacy API keys for predictions charts."""

    converted: list[dict[str, object]] = []
    for row in records:
        item: dict[str, object] = {}
        if "month" in row:
            item["month"] = row["month"]
        for legacy_key, new_key in LEGACY_PREDICTION_OUTPUT.items():
            item[legacy_key] = row.get(new_key)
        converted.append(item)
    return converted



def _collect_historical_monthly(
    as_of: str, *, meterid: str | None = None
) -> list[dict[str, object]]:
    datastore = get_datastore()
    date_col = current_app.config.get("DATE_COL", "od_date")
    cutoff = _normalize_month_end(as_of)

    if cutoff is None:
        return []

    clauses = [f"{date_col} IS NOT NULL", f"{date_col} <= ?"]
    params: list[object] = [cutoff.to_pydatetime()]

    if meterid:
        clauses.append("meterid IS NOT NULL")
        clauses.append("CAST(meterid AS VARCHAR) = ?")
        params.append(str(meterid))

    where_sql = " AND ".join(clauses)
    sql = f"""
        SELECT
            CAST(date_trunc('month', {date_col}) AS DATE) AS month,
            SUM(ocd_energy)   AS ocd_energy,
            SUM(ocd_cash_received)   AS ocd_cash_received,
            SUM(ocd_paymoney) AS ocd_paymoney
        FROM merged_sales_customers_clean
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY 1
    """

    try:
        monthly = datastore.run_query(sql, params)
        return _serialize_monthly(monthly, "month")
    except Exception:
        current_app.logger.exception(
            "Historical monthly aggregation failed; falling back to pandas"
        )

    base = datastore.get(copy=True)
    if date_col not in base.columns:
        return []

    frame = base.copy()
    frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce")
    frame = frame.dropna(subset=[date_col])

    if meterid:
        meter_col = next(
            (col for col in frame.columns if str(col).lower() == "meterid"),
            None,
        )
        if meter_col:
            frame = frame[frame[meter_col].astype(str) == str(meterid)]

    if frame.empty:
        return []

    frame = frame[frame[date_col] <= cutoff]
    metric_cols = [col for col in METRIC_COLUMNS if col in frame.columns]
    if not metric_cols:
        return []

    monthly = (
        frame.assign(month=frame[date_col].dt.to_period("M").dt.to_timestamp("M"))
        .groupby("month", as_index=False)[metric_cols]
        .sum(min_count=1)
    )

    return _serialize_monthly(monthly, "month")


def _collect_forecast_monthly(predictions_df: pd.DataFrame) -> list[dict[str, object]]:
    if predictions_df is None or predictions_df.empty:
        return []

    if "forecast_date" not in predictions_df.columns:
        if "prediction_date" in predictions_df.columns:
            predictions_df = predictions_df.rename(
                columns={"prediction_date": "forecast_date"}
            )
        else:
            return []

    metrics = [col for col in METRIC_COLUMNS if col in predictions_df.columns]
    if not metrics:
        return []

    monthly = (
        predictions_df.copy()
        .assign(forecast_date=pd.to_datetime(predictions_df["forecast_date"], errors="coerce"))
        .dropna(subset=["forecast_date"])
    )

    if monthly.empty:
        return []

    monthly = (
        monthly.assign(forecast_month=monthly["forecast_date"].dt.to_period("M").dt.to_timestamp("M"))
        .groupby("forecast_month", as_index=False)[metrics]
        .sum(min_count=1)
    )

    monthly = monthly.rename(columns={"forecast_month": "month"})
    return _serialize_monthly(monthly, "month")


def _get_meter_last_date(meterid: str) -> str | None:
    if not meterid:
        return None

    datastore = get_datastore()
    date_col = current_app.config.get("DATE_COL", "od_date")

    try:
        rows = datastore.run_query(
            f"""
            SELECT MAX({date_col}) AS last_date
            FROM merged_sales_customers_clean
            WHERE {date_col} IS NOT NULL
              AND meterid IS NOT NULL
              AND CAST(meterid AS VARCHAR) = ?
            """,
            [str(meterid)],
        )
        if not rows.empty:
            value = rows.at[0, "last_date"] if "last_date" in rows.columns else None
            ts = pd.to_datetime(value, errors="coerce")
            if pd.notna(ts):
                return ts.date().isoformat()
    except Exception:
        current_app.logger.exception("Unable to determine last date for meter via SQL")

    base = datastore.get(copy=True)
    if base.empty:
        return None

    date_series = None
    meter_col = next((col for col in base.columns if str(col).lower() == "meterid"), None)
    date_col_ref = next((col for col in base.columns if str(col).lower() == str(date_col).lower()), None)

    if meter_col and date_col_ref:
        try:
            subset = base[base[meter_col].astype(str) == str(meterid)]
            if subset.empty:
                return None
            date_series = pd.to_datetime(subset[date_col_ref], errors="coerce").dropna()
        except Exception:
            date_series = None

    if date_series is not None and not date_series.empty:
        last_date = date_series.max()
        if pd.notna(last_date):
            return last_date.date().isoformat()

    return None

def _get_dataset_last_date() -> str | None:
    datastore = get_datastore()
    date_col = current_app.config.get("DATE_COL", "od_date")

    try:
        rows = datastore.run_query(
            f"""
            SELECT MAX({date_col}) AS last_date
            FROM merged_sales_customers_clean
            WHERE {date_col} IS NOT NULL
            """
        )
        if not rows.empty:
            value = rows.at[0, "last_date"] if "last_date" in rows.columns else None
            ts = pd.to_datetime(value, errors="coerce")
            if pd.notna(ts):
                return ts.date().isoformat()
    except Exception:
        current_app.logger.exception("Unable to determine last date for dataset via SQL")

    base = datastore.get(copy=True)
    if base.empty:
        return None

    date_col_ref = next(
        (col for col in base.columns if str(col).lower() == str(date_col).lower()),
        None,
    )

    if date_col_ref:
        try:
            date_series = pd.to_datetime(base[date_col_ref], errors="coerce").dropna()
        except Exception:
            date_series = pd.Series(dtype="datetime64[ns]")

        if not date_series.empty:
            last_date = date_series.max()
            if pd.notna(last_date):
                return last_date.date().isoformat()

    return None




def index():
    date_col = (
        current_app.config()["DATE_COL"] if callable(current_app.config) else current_app.config["DATE_COL"]
    )
    datastore = get_datastore()
    metrics = get_metrics()
    base = datastore.get(copy=True)

    if getattr(datastore, "_df", None) is None or base.empty:
        return render_template("upload.html")

    if request.args and no_filters_selected(request.args, base):
        return redirect(url_for("dashboard.index"))

    params = build_params(request.args, base)
    after = params.apply(base, date_col)

    unique_values = build_unique_values(after)

    meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)

    if "meterid" in base.columns:
        try:
            meterids = datastore.run_query(
                f"""
                SELECT DISTINCT meterid AS v
                FROM merged_sales_customers_clean
                WHERE meterid IS NOT NULL
                ORDER BY v
                LIMIT {int(meter_cap)};
                """
            )["v"].astype(str).tolist()
            unique_values["meterid"] = meterids
        except Exception:
            pass

    if "utility" in base.columns:
        try:
            clause, sql_params = params.to_sql_where(
                date_col=date_col,
                available_columns=base.columns,
            )

            locs = datastore.run_query(
                f"""
                SELECT DISTINCT CAST(utility AS VARCHAR) AS v
                FROM merged_sales_customers_clean
                WHERE {clause} AND utility IS NOT NULL
                ORDER BY v;
                """,
                sql_params,
            )["v"].astype(str).tolist()
            unique_values["utility"] = locs
        except Exception:
            pass

    start_value = end_value = ""
    if date_col in after.columns and len(after) > 0:
        dmin = pd.to_datetime(after[date_col], errors="coerce").min()
        dmax = pd.to_datetime(after[date_col], errors="coerce").max()
        if pd.notna(dmin):
            start_value = dmin.date().isoformat()
        if pd.notna(dmax):
            end_value = dmax.date().isoformat()

    stats = datastore.compute_stats(after)
    summary = datastore.compute_summary(after)

    chart_metrics = metrics.available(after)
    default_metric = chart_metrics[0][0] if chart_metrics else ""

    preview_html = after.head(10).to_html(
        classes="table table-sm table-striped table-hover", index=False
    )

    return render_template(
        "index.html",
        date_col=date_col,
        stats=stats,
        summary=summary,
        start_value=start_value,
        end_value=end_value,
        unique_values=unique_values,
        args=request.args,
        total_rows=len(after),
        total_cols=len(after.columns),
        preview_html=preview_html,
        chart_metrics=chart_metrics,
        default_metric=default_metric,
    )

def predictions():
    datastore = get_datastore()
    base = datastore.get(copy=True)

    meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)
    meter_options: list[str] = []

    if getattr(datastore, "_df", None) is not None and not base.empty:
        cols_lc = {str(col).lower(): col for col in base.columns}
        meter_column = cols_lc.get("meterid")

        if meter_column:
            try:
                rows = datastore.run_query(
                    f"""
                    SELECT DISTINCT meterid AS v
                    FROM merged_sales_customers_clean
                    WHERE meterid IS NOT NULL
                    ORDER BY v
                    LIMIT {int(meter_cap)};
                    """
                )
                meter_options = rows["v"].astype(str).tolist()
            except Exception:
                try:
                    series = base[meter_column].dropna().astype(str)
                    meter_options = sorted(series.unique().tolist())[: int(meter_cap)]
                except Exception:
                    meter_options = []

    return render_template(
        "predictions.html",
        meter_options=meter_options,
        meterid_limit=int(meter_cap),
    )


def _run_predict_all():
    predictor = get_predictor()
    return _prepare_predictions_df(predictor.predict_all_from_db())



def _run_predict_one(meterid: int):
    predictor = get_predictor()
    return _prepare_predictions_df(
        predictor.predict_one_meter_from_db(meterid=meterid)
    )


def predictions_predict_all():
    try:
        predictions_df = _run_predict_all()
    except Exception:  # pragma: no cover - defensive logging
        current_app.logger.exception("Predict All request failed")
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Unable to generate predictions at this time.",
                }
            ),
            500,
        )

    if predictions_df is None:
        predictions_df = pd.DataFrame()

    as_of = _extract_as_of(predictions_df)
    if not as_of:
        as_of = _get_dataset_last_date()

    forecast_monthly = _convert_to_legacy_metrics(
        _collect_forecast_monthly(predictions_df)
    )
    historical_monthly: list[dict[str, object]] = []
    if forecast_monthly:
        historical_monthly = _convert_to_legacy_metrics(
            _collect_historical_monthly(as_of)
        )

    row_count = int(len(predictions_df))
    preview_rows = min(PREVIEW_ROW_LIMIT, row_count)
    preview_html = ""

    if row_count:
        preview_html = _render_prediction_preview_table(
            predictions_df, preview_rows
        )

    return jsonify(
        {
            "ok": True,
            "row_count": row_count,
            "preview_rows": preview_rows,
            "preview_html": preview_html,
            "as_of": as_of,
            "scope": "all",
            "charts": {
                "historical": historical_monthly,
                "forecast": forecast_monthly,
            },
        }
    )


def predictions_predict_one():
    payload = request.get_json(silent=True) or {}
    meterid_raw = str(payload.get("meterid", "")).strip()

    if not meterid_raw:
        return (
            jsonify({"ok": False, "error": "Select a meter before running Predict."}),
            200,
        )

    try:
        meterid_int = int(meterid_raw)
    except (TypeError, ValueError):
        return (
            jsonify({"ok": False, "error": "Meter ID must be a valid number."}),
            200,
        )

    as_of = _get_meter_last_date(meterid_raw)
    if not as_of:
        return (
            jsonify({"ok": False, "error": "No historical data found for the selected meter."}),
            200,
        )

    try:
        predictions_df = _run_predict_one(meterid_int)
    except Exception:  # pragma: no cover - defensive logging
        current_app.logger.exception("Predict meter request failed")
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Unable to generate predictions at this time.",
                }
            ),
            200,
        )

    if predictions_df is None:
        predictions_df = pd.DataFrame()

    inferred_as_of = _extract_as_of(predictions_df)
    if inferred_as_of:
        as_of = inferred_as_of


    forecast_monthly = _convert_to_legacy_metrics(
        _collect_forecast_monthly(predictions_df)
    )
    historical_monthly: list[dict[str, object]] = []
    if forecast_monthly:
        historical_monthly = _convert_to_legacy_metrics(
            _collect_historical_monthly(as_of, meterid=meterid_raw)
        )

    row_count = int(len(predictions_df))
    preview_rows = min(PREVIEW_ROW_LIMIT, row_count)
    preview_html = ""

    if row_count:
        preview_html = _render_prediction_preview_table(
            predictions_df, preview_rows
        )

    return jsonify(
        {
            "ok": True,
            "row_count": row_count,
            "preview_rows": preview_rows,
            "preview_html": preview_html,
            "as_of": as_of,
            "meterid": meterid_raw,
            "scope": "meter",
            "charts": {
                "historical": historical_monthly,
                "forecast": forecast_monthly,
            },
        }
    )


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
        except Exception:  # pragma: no cover - defensive logging
            current_app.logger.exception("Predict meter download failed")
            return make_response("Unable to generate predictions", 500)

        if predictions_df is None:
            predictions_df = pd.DataFrame()

        filename = f"predict_meter_{meterid_raw}.csv"
    else:
        as_of = PREDICT_ALL_AS_OF

        try:
            predictions_df = _run_predict_all()
        except Exception:  # pragma: no cover - defensive logging
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


bp.add_url_rule("/predictions", view_func=predictions, methods=["GET"])
bp.add_url_rule("/predictions/api/predict-all", view_func=predictions_predict_all, methods=["POST"])
bp.add_url_rule("/predictions/api/predict", view_func=predictions_predict_one, methods=["POST"])
bp.add_url_rule("/predictions/download", view_func=predictions_download, methods=["GET"])
bp.add_url_rule("/", view_func=index, methods=["GET"])
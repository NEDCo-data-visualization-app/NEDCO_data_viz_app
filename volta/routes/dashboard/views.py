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

from . import bp, get_datastore, get_metrics, get_predictor

from . import bp, get_datastore, get_metrics
from .helpers import DEFAULT_METERID_LIMIT, build_params, build_unique_values, no_filters_selected

PREDICT_ALL_AS_OF = "09-2020"
PREVIEW_ROW_LIMIT = 10
METRIC_COLUMNS = ["kwh", "ghc", "paymoney"]


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


def _collect_historical_monthly(
    as_of: str, *, meterid: str | None = None
) -> list[dict[str, object]]:
    datastore = get_datastore()
    date_col = current_app.config.get("DATE_COL", "chargedate")
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
            SUM(kwh)   AS kwh,
            SUM(ghc)   AS ghc,
            SUM(paymoney) AS paymoney
        FROM prod.sales
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
    date_col = current_app.config.get("DATE_COL", "chargedate")

    try:
        rows = datastore.run_query(
            f"""
            SELECT MAX({date_col}) AS last_date
            FROM prod.sales
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
                FROM prod.sales
                WHERE meterid IS NOT NULL
                ORDER BY v
                LIMIT {int(meter_cap)};
                """
            )["v"].astype(str).tolist()
            unique_values["meterid"] = meterids
        except Exception:
            pass

    if "loc" in base.columns:
        try:
            clause, sql_params = params.to_sql_where(
                date_col=date_col,
                available_columns=base.columns,
            )

            locs = datastore.run_query(
                f"""
                SELECT DISTINCT CAST(loc AS VARCHAR) AS v
                FROM prod.sales
                WHERE {clause} AND loc IS NOT NULL
                ORDER BY v;
                """,
                sql_params,
            )["v"].astype(str).tolist()
            unique_values["loc"] = locs
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
                    FROM prod.sales
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


def _run_predict_all(as_of: str = PREDICT_ALL_AS_OF):
    predictor = get_predictor()
    return predictor.predict_recursive(as_of=as_of)


def _run_predict_one(meterid: int, as_of: str):
    predictor = get_predictor()
    return predictor.predict_recursive_one(meterid=meterid, as_of=as_of)


def predictions_predict_all():
    as_of = PREDICT_ALL_AS_OF

    try:
        predictions_df = _run_predict_all(as_of)
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

    forecast_monthly = _collect_forecast_monthly(predictions_df)
    historical_monthly: list[dict[str, object]] = []
    if forecast_monthly:
        historical_monthly = _collect_historical_monthly(as_of)

    row_count = int(len(predictions_df))
    preview_rows = min(PREVIEW_ROW_LIMIT, row_count)
    preview_html = ""

    if row_count:
        preview_html = (
            predictions_df.head(preview_rows)
            .to_html(
                classes="table table-sm table-striped table-hover mb-0",
                index=False,
                border=0,
            )
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
        predictions_df = _run_predict_one(meterid_int, as_of)
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

    forecast_monthly = _collect_forecast_monthly(predictions_df)
    historical_monthly: list[dict[str, object]] = []
    if forecast_monthly:
        historical_monthly = _collect_historical_monthly(as_of, meterid=meterid_raw)

    row_count = int(len(predictions_df))
    preview_rows = min(PREVIEW_ROW_LIMIT, row_count)
    preview_html = ""

    if row_count:
        preview_html = (
            predictions_df.head(preview_rows)
            .to_html(
                classes="table table-sm table-striped table-hover mb-0",
                index=False,
                border=0,
            )
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
            predictions_df = _run_predict_one(meterid_int, as_of)
        except Exception:  # pragma: no cover - defensive logging
            current_app.logger.exception("Predict meter download failed")
            return make_response("Unable to generate predictions", 500)

        if predictions_df is None:
            predictions_df = pd.DataFrame()

        filename = f"predict_meter_{meterid_raw}.csv"
    else:
        as_of = PREDICT_ALL_AS_OF

        try:
            predictions_df = _run_predict_all(as_of)
        except Exception:  # pragma: no cover - defensive logging
            current_app.logger.exception("Predict All download failed")
            return make_response("Unable to generate predictions", 500)

        if predictions_df is None:
            predictions_df = pd.DataFrame()

        filename = "predict_all.csv"

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
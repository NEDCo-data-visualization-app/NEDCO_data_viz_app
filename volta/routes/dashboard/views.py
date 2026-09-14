"""Page views: dashboard index and the predictions page.

The predictions page only reads the ``predict_all_cache`` table. That table is
filled by ``POST /predictions/api/predict-all-cache`` (which runs the LightGBM
models) or by a pre-populated warehouse file.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
from flask import current_app, jsonify, make_response, render_template, request
from markupsafe import escape
from werkzeug.datastructures import ImmutableMultiDict

from ..auth import effective_public_mode
from . import bp, get_datastore, get_metrics, get_predictor
from .downloads import csv_response
from .helpers import DEFAULT_METERID_LIMIT, build_params, build_unique_values

PREVIEW_ROW_LIMIT = 10
SENSITIVE_COLUMNS = {"meterid", "customer_no"}
NUMERIC_PREDICTION_COLUMNS = {"paymoney_pred", "energy_pred", "cash_pred"}

COLUMN_LABELS = {
    "meterid": "Meter ID",
    "customer_no": "Customer No.",
    "utility": "Location",
    "tariff_type": "Account Type",
    "as_of": "As Of",
    "prediction_date": "Forecast Month",
    "horizon": "Horizon (Months Ahead)",
    "energy_pred": "Energy (kWh)",
    "cash_pred": "Cash Received (GHC)",
    "paymoney_pred": "Paymoney",
}

COLUMN_CLASSES = {
    "meterid": "text-nowrap text-center",
    "horizon": "text-nowrap text-center",
    "energy_pred": "text-nowrap text-end",
    "cash_pred": "text-nowrap text-end",
    "paymoney_pred": "text-nowrap text-end",
    "ocd_energy": "text-nowrap text-end",
    "ocd_cash_received": "text-nowrap text-end",
    "ocd_paymoney": "text-nowrap text-end",
}


def _is_public(override: Optional[bool] = None) -> bool:
    """Public mode for this request.

    Routes may force public (override=True), which always wins. Asking for the
    private view (override=False) only works when the deployment runs in
    private mode or the session signed in with the private password.
    """
    if override:
        return True
    return effective_public_mode()


def _forecast_as_of() -> str:
    return str(current_app.config.get("FORECAST_AS_OF", "2020-09-01"))


# --------------------------------------------------------------------- tables
def _format_value(column: str, value: Any) -> str:
    if value is None:
        return "—"
    try:
        if pd.isna(value):
            return "—"
    except (TypeError, ValueError):
        pass
    if column == "horizon":
        try:
            return str(int(float(value)))
        except (TypeError, ValueError):
            return str(escape(str(value)))
    if column in NUMERIC_PREDICTION_COLUMNS or column in get_metrics().mapping:
        try:
            return f"{float(value):,.2f}"
        except (TypeError, ValueError):
            return str(escape(str(value)))
    return str(escape(str(value)))


def _render_preview_table(rows: List[Dict[str, Any]], limit: int = PREVIEW_ROW_LIMIT, is_public: bool = False) -> str:
    """Render a list of dicts as a Bootstrap table, hiding identifiers in public mode."""
    rows = rows[:limit]
    if not rows:
        return ""

    columns = [c for c in rows[0].keys() if not (is_public and c.lower() in SENSITIVE_COLUMNS)]
    labels = dict(COLUMN_LABELS)
    labels.update(get_metrics().mapping)

    head = "".join(
        f'<th scope="col" class="{COLUMN_CLASSES.get(col, "text-nowrap")}">{escape(labels.get(col, col))}</th>'
        for col in columns
    )
    body = "".join(
        "<tr>"
        + "".join(f'<td class="{COLUMN_CLASSES.get(col, "text-nowrap")}">{_format_value(col, row.get(col))}</td>' for col in columns)
        + "</tr>"
        for row in rows
    )
    return (
        '<table class="table table-sm table-striped table-hover align-middle mb-0">'
        f'<thead class="table-light"><tr>{head}</tr></thead><tbody>{body}</tbody></table>'
    )


# ------------------------------------------------------------------ dashboard
def index(first_load_override: Optional[bool] = None, is_public: Optional[bool] = None):
    datastore = get_datastore()
    metrics = get_metrics()
    public = _is_public(is_public)
    date_col = current_app.config.get("DATE_COL", "od_date")

    columns = datastore.get_columns()
    if not columns:
        return render_template("upload.html")

    args = ImmutableMultiDict() if first_load_override else request.args
    params = build_params(args, base_columns=columns)
    if params.selections or params.start or params.end:
        clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=columns)
    else:
        clause, sql_params = "", []

    preview_sql = f"SELECT * FROM {datastore.table_sql}"
    if clause:
        preview_sql += f" WHERE {clause}"
    preview_sql += f" ORDER BY {date_col} DESC LIMIT {PREVIEW_ROW_LIMIT}"
    preview_rows = datastore.run_query(preview_sql, sql_params)

    stats = datastore.compute_stats(where_clause=clause, sql_params=sql_params)
    summary = datastore.compute_summary(where_clause=clause, sql_params=sql_params)

    facet_cols = ["utility", "tariff_type"] if public else ["meterid", "utility", "tariff_type"]
    unique_values = build_unique_values(
        datastore, facet_cols, clause, sql_params,
        max_uniques=current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT),
    )

    chart_metrics = metrics.available([dict.fromkeys(columns)])
    default_metric = chart_metrics[0][0] if chart_metrics else ""

    return render_template(
        "index.html",
        date_col=date_col,
        stats=stats,
        summary=summary,
        start_value=args.get("start_date") or summary.get("date_min", ""),
        end_value=args.get("end_date") or summary.get("date_max", ""),
        unique_values=unique_values,
        args=args,
        total_rows=summary.get("rows", 0),
        total_cols=len(columns),
        preview_html=_render_preview_table(preview_rows, PREVIEW_ROW_LIMIT, is_public=public),
        chart_metrics=chart_metrics,
        default_metric=default_metric,
        is_public=public,
    )


# ---------------------------------------------------------------- predictions
def _ensure_predict_all_cache_table(datastore) -> None:
    datastore.execute(
        """
        CREATE TABLE IF NOT EXISTS predict_all_cache (
            meterid         VARCHAR,
            as_of           DATE,
            horizon         INT,
            prediction_date DATE,
            paymoney_pred   DOUBLE,
            energy_pred     DOUBLE,
            cash_pred       DOUBLE,
            utility         VARCHAR,
            PRIMARY KEY (meterid, as_of, prediction_date, horizon)
        )
        """
    )


def _scope_where(meterid: Optional[str], utilities: Optional[List[str]], extra: Optional[List[str]] = None):
    """WHERE fragment + params restricting to a meter and/or a set of locations."""
    clauses = list(extra or [])
    params: List[Any] = []
    if meterid:
        clauses.append("CAST(meterid AS VARCHAR) = ?")
        params.append(str(meterid))
    if utilities:
        clauses.append(f"utility IN ({', '.join('?' for _ in utilities)})")
        params.extend(utilities)
    return (" AND ".join(clauses) if clauses else "1=1"), params


def _monthly(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "month": r["month"].isoformat() if r.get("month") else None,
            "kwh": float(r.get("kwh") or 0),
            "paymoney": float(r.get("paymoney") or 0),
            "ghc": float(r.get("ghc") or 0),
        }
        for r in rows
    ]


def _historical_monthly(as_of: str, meterid: Optional[str], utilities: Optional[List[str]]):
    datastore = get_datastore()
    date_col = current_app.config.get("DATE_COL", "od_date")
    where, params = _scope_where(meterid, utilities, extra=[f"{date_col} <= ?"])
    sql = f"""
        SELECT DATE_TRUNC('month', {date_col})::DATE AS month,
               SUM(ocd_energy) AS kwh, SUM(ocd_paymoney) AS paymoney, SUM(ocd_cash_received) AS ghc
        FROM {datastore.table_sql}
        WHERE {where}
        GROUP BY 1 ORDER BY 1
    """
    return _monthly(datastore.run_query(sql, [as_of, *params]))


def _forecast_monthly(as_of: str, meterid: Optional[str], utilities: Optional[List[str]]):
    datastore = get_datastore()
    where, params = _scope_where(
        meterid, utilities,
        extra=["prediction_date > ?", "(energy_pred != 0 OR paymoney_pred != 0 OR cash_pred != 0)"],
    )
    sql = f"""
        SELECT DATE_TRUNC('month', prediction_date)::DATE AS month,
               SUM(energy_pred) AS kwh, SUM(paymoney_pred) AS paymoney, SUM(cash_pred) AS ghc
        FROM predict_all_cache
        WHERE {where}
        GROUP BY 1 ORDER BY 1
    """
    return _monthly(datastore.run_query(sql, [as_of, *params]))


def _cached_preview(meterid: Optional[str], utilities: Optional[List[str]], limit: int = PREVIEW_ROW_LIMIT):
    where, params = _scope_where(meterid, utilities)
    sql = f"SELECT * FROM predict_all_cache WHERE {where} ORDER BY meterid, prediction_date LIMIT ?"
    return get_datastore().run_query(sql, [*params, limit])


def _cached_count(meterid: Optional[str], utilities: Optional[List[str]]) -> int:
    where, params = _scope_where(meterid, utilities)
    row = get_datastore().fetch_one(f"SELECT COUNT(*) AS n FROM predict_all_cache WHERE {where}", params)
    return int(row["n"]) if row else 0


def _prediction_payload(meterid: Optional[str], utilities: Optional[List[str]]) -> Dict[str, Any]:
    datastore = get_datastore()
    _ensure_predict_all_cache_table(datastore)
    as_of = _forecast_as_of()
    preview_rows = _cached_preview(meterid, utilities)
    return {
        "ok": True,
        "row_count": _cached_count(meterid, utilities),
        "preview_rows": len(preview_rows),
        "preview_html": _render_preview_table(preview_rows, PREVIEW_ROW_LIMIT, is_public=_is_public()),
        "as_of": as_of,
        "meterid": meterid,
        "scope": "meter" if meterid else "all",
        "charts": {
            "historical": _historical_monthly(as_of, meterid, utilities),
            "forecast": _forecast_monthly(as_of, meterid, utilities),
        },
    }


def _request_scope():
    payload = request.get_json(silent=True) or {}
    utilities = payload.get("utility") or []
    if not isinstance(utilities, list):
        utilities = [utilities]
    utilities = [str(u) for u in utilities if u not in (None, "")]
    meterid = payload.get("meterid")
    meterid = str(meterid).strip() if meterid not in (None, "") else None
    if _is_public():
        meterid = None
    return meterid, utilities or None


def predictions(is_public: Optional[bool] = None):
    datastore = get_datastore()
    public = _is_public(is_public)
    columns = datastore.get_columns()

    meter_options: List[str] = []
    if not public and "meterid" in columns:
        limit = int(current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT))
        rows = datastore.run_query(
            f"SELECT DISTINCT CAST(meterid AS VARCHAR) AS v FROM {datastore.table_sql} ORDER BY v LIMIT {limit}"
        )
        meter_options = [r["v"] for r in rows]

    location_options: List[str] = []
    if "utility" in columns:
        rows = datastore.run_query(f"SELECT DISTINCT utility AS v FROM {datastore.table_sql} WHERE utility IS NOT NULL ORDER BY v")
        location_options = [r["v"] for r in rows]

    return render_template(
        "predictions.html",
        is_public=public,
        meter_options=meter_options,
        meterid_limit=50,
        location_options=location_options,
        selected_locations=request.args.getlist("utility"),
        selected_meter=None if public else request.args.get("meterid"),
        preview_html="<div class='text-muted small'>No predictions yet</div>",
        args=request.args,
    )


def predictions_predict_all():
    meterid, utilities = _request_scope()
    return jsonify(_prediction_payload(meterid, utilities))


def predictions_predict_one():
    meterid, utilities = _request_scope()
    if not meterid:
        return jsonify({"ok": False, "error": "Select a meter"}), 400
    return jsonify(_prediction_payload(meterid, utilities))


def _add_location_to_predictions(preds_df: pd.DataFrame) -> pd.DataFrame:
    datastore = get_datastore()
    mapping = pd.DataFrame(
        datastore.run_query(f"SELECT CAST(meterid AS VARCHAR) AS meterid, utility FROM {datastore.table_sql} GROUP BY 1, 2")
    )
    preds_df = preds_df.copy()
    preds_df["meterid"] = preds_df["meterid"].astype(str)
    if mapping.empty:
        preds_df["utility"] = None
        return preds_df
    mapping = mapping.drop_duplicates("meterid")
    return preds_df.merge(mapping, on="meterid", how="left")


def _cache_predict_all(datastore, preds_df: pd.DataFrame) -> int:
    """Replace the contents of predict_all_cache with a fresh prediction frame."""
    _ensure_predict_all_cache_table(datastore)
    df = preds_df.copy()
    df["meterid"] = df["meterid"].astype(str)
    df["horizon"] = df["horizon"].astype(int)
    for col in ("as_of", "prediction_date"):
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    for col in ("paymoney_pred", "energy_pred", "cash_pred"):
        df[col] = df[col].astype(float)
    if "utility" not in df.columns:
        df["utility"] = None
    df = df.dropna(subset=["meterid", "as_of", "prediction_date", "horizon"])
    df = df.drop_duplicates(subset=["meterid", "as_of", "prediction_date", "horizon"], keep="last")

    cols = ["meterid", "as_of", "horizon", "prediction_date", "paymoney_pred", "energy_pred", "cash_pred", "utility"]
    records = [tuple(None if pd.isna(v) else v for v in row) for row in df[cols].itertuples(index=False, name=None)]

    with datastore._lock:
        datastore._con.execute("DELETE FROM predict_all_cache")
        if records:
            datastore._con.executemany("INSERT INTO predict_all_cache VALUES (?, ?, ?, ?, ?, ?, ?, ?)", records)
    return len(records)


def predictions_predict_all_cached():
    """Run the LightGBM models for every meter, store the result in the cache, and return the preview."""
    datastore = get_datastore()
    try:
        preds_df = get_predictor().predict_all_from_db()
        preds_df = _add_location_to_predictions(preds_df)
        cached = _cache_predict_all(datastore, preds_df)
    except Exception:  # noqa: BLE001
        current_app.logger.exception("Predict All failed")
        return jsonify({"ok": False, "error": "Unable to generate predictions"}), 500

    meterid, utilities = _request_scope()
    payload = _prediction_payload(meterid, utilities)
    payload["cached_rows"] = cached
    return jsonify(payload)


def predictions_download():
    datastore = get_datastore()
    _ensure_predict_all_cache_table(datastore)

    meterid = (request.args.get("meterid") or "").strip() or None
    if _is_public():
        meterid = None
    utilities = [u for u in request.args.getlist("utility") if u] or None

    where, params = _scope_where(meterid, utilities)
    sql = f"SELECT * FROM predict_all_cache WHERE {where} ORDER BY meterid, prediction_date"
    filename = f"predict_meter_{meterid}.csv" if meterid else "predict_all.csv"
    return csv_response(datastore.stream_query(sql, params), filename)


# -------------------------------------------------------------------- routes
bp.add_url_rule("/", view_func=index, methods=["GET"])
bp.add_url_rule("/public-dashboard", view_func=lambda: index(is_public=True), methods=["GET"], endpoint="public_dashboard")
bp.add_url_rule("/reset-dashboard", view_func=lambda: index(first_load_override=True), methods=["POST"], endpoint="reset_dashboard")
bp.add_url_rule("/predictions/private", view_func=lambda: predictions(is_public=False), methods=["GET"], endpoint="predictions_private")
bp.add_url_rule("/predictions", view_func=lambda: predictions(is_public=True), methods=["GET"], endpoint="predictions_public")
bp.add_url_rule("/predictions/download", view_func=predictions_download, methods=["GET"])
bp.add_url_rule("/predictions/api/predict-all", view_func=predictions_predict_all, methods=["POST"])
bp.add_url_rule("/predictions/api/predict", view_func=predictions_predict_one, methods=["POST"])
bp.add_url_rule("/predictions/api/predict-all-cache", view_func=predictions_predict_all_cached, methods=["POST"])

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
from flask import request, render_template
import pandas as pd
import os
tracemalloc.start()
PREDICT_ALL_AS_OF = "2020-09-01"
PREVIEW_ROW_LIMIT = 10
METRIC_COLUMNS = ["ocd_energy", "ocd_cash_received", "ocd_paymoney"]

LEGACY_PREDICTION_OUTPUT_HISTORICAL = {
    "kwh": "kwh",
    "paymoney": "paymoney",
    "ghc": "ghc",
}

LEGACY_PREDICTION_OUTPUT_FORECAST = {
    "kwh": "energy_pred",
    "paymoney": "paymoney_pred",
    "ghc": "cash_pred",
}


COLUMN_LABEL_DEFAULTS = {
    "meterid": "Meter ID",
    "utility":"Location",
    "as_of":"As Of",
    "prediction_date": "Forecast Month",
    "horizon": "Horizon (Months Ahead)",
    "energy_pred": "Energy (kWh)",
    "cash_pred": "Cash Received (GHC)",
    "paymoney_pred": "Paymoney",
}

COLUMN_CLASS_MAP = {
    "meterid": "text-nowrap text-center",
    "forecast_date": "text-nowrap",
    "horizon": "text-nowrap text-center",
    "ocd_energy": "text-end",
    "ocd_cash_received": "text-end",
    "ocd_paymoney": "text-nowrap text-end",
}

_CURRENT_DASHBOARD_STATE: dict | None = None

MODE = os.environ.get("PUBLIC_MODE", "False").lower() == "true"

def _set_dashboard_state(state: dict):
    global _CURRENT_DASHBOARD_STATE
    _CURRENT_DASHBOARD_STATE = state

'''
def _set_current_predictions(df: pd.DataFrame):
    """Overwrite the memory slot with new predictions."""
    global _CURRENT_PREDICTIONS
    # Delete old copy to free memory
    if _CURRENT_PREDICTIONS is not None:
        del _CURRENT_PREDICTIONS
        import gc
        gc.collect()
    _CURRENT_PREDICTIONS = df.copy()

def _get_current_predictions(filtered_meterid: int | None = None, filtered_utilities: list[str] | None = None):
    """Return predictions from memory, optionally filtered, without duplicating the full dataset."""
    if _CURRENT_PREDICTIONS is None:
        return pd.DataFrame()  # empty

    df = _CURRENT_PREDICTIONS

    if filtered_meterid is not None:
        df = df[df["meterid"].astype(str) == str(filtered_meterid)]
    if filtered_utilities:
        df = df[df["utility"].isin(filtered_utilities)]
    
    return df
'''

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


def _collect_historical_monthly(as_of: str, meterid: int | None = None, utilities: list[str] | None = None):
    datastore = get_datastore()
    date_col = current_app.config.get("DATE_COL", "od_date")

    where_clauses = [f"{date_col} <= ?"]
    params = [as_of]

    if meterid is not None:
        where_clauses.append("CAST(meterid AS BIGINT) = ?")
        params.append(int(meterid))

    if utilities:
        placeholders = ", ".join("?" for _ in utilities)
        where_clauses.append(f"utility IN ({placeholders})")
        params.extend(utilities)

    where_sql = " AND ".join(where_clauses)

    sql = f"""
        SELECT
            DATE_TRUNC('month', {date_col})::DATE AS month,
            SUM(ocd_energy)        AS kwh,
            SUM(ocd_paymoney)      AS paymoney,
            SUM(ocd_cash_received) AS ghc
        FROM "{current_app.config['PARQUET_PATH']}"
        WHERE {where_sql}
        GROUP BY month
        ORDER BY month
    """
    rows = datastore._con.execute(sql, params).fetchall()

    return [
        {"month": r[0].isoformat(), "kwh": float(r[1] or 0), "paymoney": float(r[2] or 0), "ghc": float(r[3] or 0)}
        for r in rows
    ]


def _get_cached_predict_all_raw(datastore, meterid: str | None = None, utilities: list[str] | None = None):
    """
    Fetch cached predictions from DuckDB with optional meterid and utility filters.
    """
    _ensure_predict_all_cache_table(datastore)

    sql = "SELECT * FROM predict_all_cache"
    where_clauses = []
    params: list[object] = []

    if meterid is not None:
        where_clauses.append("CAST(meterid AS VARCHAR) = ?")
        params.append(str(meterid))

    if utilities:
        # Handle multiple utilities with an IN clause
        placeholders = ", ".join("?" for _ in utilities)
        where_clauses.append(f"utility IN ({placeholders})")
        params.extend(utilities)

    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)

    sql += " ORDER BY meterid, prediction_date"

    print(f"[DEBUG] _get_cached_predict_all_raw SQL: {sql}")
    print(f"[DEBUG] _get_cached_predict_all_raw params: {params}")

    rows = datastore._con.execute(sql, params).fetchall()
    if not rows:
        return []

    cols = [d[0] for d in datastore._con.description]
    return [dict(zip(cols, r)) for r in rows]




def _collect_forecast_monthly(meterid: int | None = None, utilities: list[str] | None = None) -> list[dict[str, object]]:
    """
    Return monthly aggregated forecast metrics (kWh, paymoney, GHC)
    optionally filtered by meter ID and/or utilities.
    """
    datastore = get_datastore()

    where_clauses = ["prediction_date > DATE '2020-08-31'",
                     "(energy_pred != 0 OR paymoney_pred != 0 OR cash_pred != 0)"]
    params: list[object] = []

    if meterid is not None:
        where_clauses.append("CAST(meterid AS BIGINT) = ?")
        params.append(int(meterid))

    if utilities:
        placeholders = ", ".join("?" for _ in utilities)
        where_clauses.append(f"utility IN ({placeholders})")
        params.extend(utilities)

    where_sql = " AND ".join(where_clauses)

    sql = f"""
        SELECT
            DATE_TRUNC('month', prediction_date)::DATE AS month,
            SUM(energy_pred)   AS kwh,
            SUM(paymoney_pred) AS paymoney,
            SUM(cash_pred)     AS ghc
        FROM predict_all_cache
        WHERE {where_sql}
        GROUP BY month
        ORDER BY month
    """

    rows = datastore._con.execute(sql, params).fetchall()

    return [
        {
            "month": r[0].isoformat(),
            "kwh": float(r[1] or 0),
            "paymoney": float(r[2] or 0),
            "ghc": float(r[3] or 0)
        }
        for r in rows
    ]







def _convert_to_legacy_metrics(records: list[dict[str, object]], mapping: dict[str, str]) -> list[dict[str, object]]:
    """Map internal metric column names to legacy API keys and format month ISO."""
    converted = []
    for row in records:
        month_val = row.get("month")
        if isinstance(month_val, (datetime.date, datetime.datetime)):
            month_val = month_val.strftime("%Y-%m-%d")
        item = {"month": str(month_val)}
        for legacy, internal in mapping.items():
            val = row.get(internal)
            item[legacy] = float(val) if val is not None else 0
        converted.append(item)
    return converted

# Add this debugging code at the start of your index() function
# This will help you see exactly what's coming through

def index(first_load_override: bool | None = None, is_public: bool = MODE):
    """Dashboard index view."""
    
    # 🔍 DEBUG: Log all incoming request parameters
    print("\n" + "="*60)
    print("[INDEX ROUTE] Request Args Debug:")
    print("="*60)
    print(f"request.method: {request.method}")
    print(f"request.url: {request.url}")
    print(f"request.query_string: {request.query_string.decode()}")
    print("\nAll args:")
    for key in request.args.keys():
        values = request.args.getlist(key)
        print(f"  {key}: {values}")
    
    # Specifically check metric
    metric_single = request.args.get('metric')
    metric_list = request.args.getlist('metric')
    print(f"\nMetric (single): {metric_single}")
    print(f"Metric (list): {metric_list}")
    print("="*60 + "\n")
    
    # ... rest of your index function
    datastore = get_datastore()
    metrics = get_metrics()
    date_col = current_app.config.get("DATE_COL", "od_date")

    # Step 1: get columns from DuckDB
    columns = datastore.get_columns()
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

    # --- Step 6: compute stats / summary ---
    stats = datastore.compute_stats(where_clause=clause, sql_params=sql_params)
    summary = datastore.compute_summary(where_clause=clause, sql_params=sql_params)

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

def predictions(is_public=False):
    datastore = get_datastore()

    # --- Meter options (private only) ---
    meter_options = []
    if not is_public:
        sql = f"""
            SELECT DISTINCT CAST(meterid AS BIGINT) AS meterid
            FROM "{current_app.config['PARQUET_PATH']}"
            ORDER BY meterid
            LIMIT {current_app.config.get('METERID_MAX_OPTIONS', 1000)}
        """
        rows = datastore._con.execute(sql).fetchall()
        meter_options = [r[0] for r in rows]

    meterid_limit = 50  # For input limit in UI

    # --- Location filter (public & private) ---
    sql = f"""
        SELECT DISTINCT utility
        FROM "{current_app.config['PARQUET_PATH']}"
        ORDER BY utility
    """
    location_rows = datastore._con.execute(sql).fetchall()
    location_options = [r[0] for r in location_rows]

    # --- Selected filters from request ---
    selected_locations = request.args.getlist("utility")
    selected_meter = request.args.get("meterid") if not is_public else None

    # --- Default preview (empty) ---
    preview_html = "<div class='text-muted small'>No predictions yet</div>"

    return render_template(
        "predictions.html",
        is_public=is_public,
        meter_options=meter_options,
        meterid_limit=meterid_limit,
        location_options=location_options,
        selected_locations=selected_locations,
        selected_meter=selected_meter,
        preview_html=preview_html,
        args=request.args,
    )


'''
def _run_predict_all():
    """
    Load all predictions into the single memory slot.
    """
    datastore = get_datastore()

    # If we already have them, reuse
    global _CURRENT_PREDICTIONS
    if _CURRENT_PREDICTIONS is not None:
        return _CURRENT_PREDICTIONS

    predictor = get_predictor()
    preds_df = predictor.predict_all_from_db()
    preds_df = _add_location_to_predictions(preds_df)

    # Store in the single memory slot
    _set_current_predictions(preds_df)

    # Cache into DuckDB if needed
    _cache_predict_all(datastore, preds_df)

    return preds_df

def _run_predict_one(meterid: int):
    """
    Load only one meter into the memory slot.
    Overwrites whatever was in _CURRENT_PREDICTIONS.
    """
    predictor = get_predictor()
    df = predictor.predict_one_meter_from_db(meterid)
    df = _add_location_to_predictions(df)

    # Overwrite the memory slot
    _set_current_predictions(df)

    return df
'''
def predictions_predict_all():
    datastore = get_datastore()
    as_of = PREDICT_ALL_AS_OF

    payload = request.get_json(silent=True) or {}
    utilities = payload.get("utility") or []
    meterid = payload.get("meterid")

    # 1️⃣ Preview rows (LIMIT 10 ONLY)
    preview_rows = _get_cached_predict_all_preview(
        datastore,
        meterid=payload.get("meterid"),
        utilities=payload.get("utility") or None
    )

    preview_html = _render_prediction_preview_table(preview_rows, PREVIEW_ROW_LIMIT)

    # 2️⃣ Charts (already aggregated in SQL)
    historical_monthly = _collect_historical_monthly(
        as_of,
        meterid=meterid,
        utilities=utilities or None
    )

    forecast_monthly = _collect_forecast_monthly(
        meterid=meterid,
        utilities=utilities or None
    )

    # 3️⃣ Count (single integer)
    row_count = _get_cached_predict_all_count(
        datastore,
        meterid=meterid,
        utilities=utilities or None
    )

    # 4️⃣ Store ONE result bundle in memory
    _set_dashboard_state({
        "filters": {
            "meterid": meterid,
            "utilities": utilities,
        },
        "preview_rows": preview_rows,
        "preview_html": preview_html,
        "historical_monthly": historical_monthly,
        "forecast_monthly": forecast_monthly,
        "row_count": row_count,
    })

    return jsonify({
        "ok": True,
        "row_count": row_count,
        "preview_rows": len(preview_rows),
        "preview_html": preview_html,
        "as_of": as_of,
        "scope": "all" if not meterid else "meter",
        "charts": {
            "historical": historical_monthly,
            "forecast": forecast_monthly,
        },
    })



def _get_cached_predict_all_preview(
    datastore,
    meterid: str | None = None,
    utilities: list[str] | None = None,
    limit: int = PREVIEW_ROW_LIMIT,
):
    _ensure_predict_all_cache_table(datastore)

    sql = "SELECT * FROM predict_all_cache"
    where_clauses = []
    params: list[object] = []

    if meterid is not None:
        where_clauses.append("meterid = ?")
        params.append(str(meterid))

    if utilities:
        placeholders = ", ".join("?" for _ in utilities)
        where_clauses.append(f"utility IN ({placeholders})")
        params.extend(utilities)

    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)

    sql += " ORDER BY meterid, prediction_date LIMIT ?"
    params.append(limit)

    rows = datastore._con.execute(sql, params).fetchall()
    if not rows:
        return []

    cols = [d[0] for d in datastore._con.description]
    return [dict(zip(cols, r)) for r in rows]
def predictions_predict_one():
    datastore = get_datastore()
    as_of = PREDICT_ALL_AS_OF

    payload = request.get_json(silent=True) or {}

    meterid = payload.get("meterid")
    utilities = payload.get("utility") or []

    if meterid is None:
        return jsonify({"ok": False, "error": "Select a meter"}), 400

    try:
        meterid = int(meterid)
    except ValueError:
        return jsonify({"ok": False, "error": "Meter ID must be an integer"}), 400

    # 1️⃣ Preview rows (LIMIT 10 ONLY)
    preview_rows = _get_cached_predict_all_preview(
        datastore,
        meterid=meterid,
        utilities=utilities or None,
        limit=PREVIEW_ROW_LIMIT,
    )

    preview_html = _render_prediction_preview_table(preview_rows, PREVIEW_ROW_LIMIT)

    # 2️⃣ Charts (aggregated in DuckDB)
    historical_monthly = _collect_historical_monthly(
        as_of,
        meterid=meterid,
        utilities=utilities or None,
    )

    forecast_monthly = _collect_forecast_monthly(
        meterid=meterid,
        utilities=utilities or None,
    )

    historical_monthly_legacy = _convert_to_legacy_metrics(
        historical_monthly, LEGACY_PREDICTION_OUTPUT_HISTORICAL
    )

    forecast_monthly_legacy = _convert_to_legacy_metrics(
        forecast_monthly, LEGACY_PREDICTION_OUTPUT_FORECAST
    )

    # 3️⃣ Count (single integer)
    row_count = _get_cached_predict_all_count(
        datastore,
        meterid=str(meterid),
        utilities=utilities or None,
    )

    # 4️⃣ Store ONE dashboard state (optional but consistent)
    _set_dashboard_state({
        "filters": {
            "meterid": meterid,
            "utilities": utilities,
        },
        "preview_rows": preview_rows,
        "preview_html": preview_html,
        "historical_monthly": historical_monthly,
        "forecast_monthly": forecast_monthly,
        "row_count": row_count,
    })

    return jsonify({
        "ok": True,
        "row_count": row_count,
        "preview_rows": len(preview_rows),
        "preview_html": preview_html,
        "as_of": as_of,
        "meterid": meterid,
        "scope": "meter",
        "charts": {
            "historical": historical_monthly_legacy,
            "forecast": forecast_monthly_legacy,
        },
    })


def _add_location_to_predictions(preds):
    # Ensure preds is a DataFrame
    if isinstance(preds, list):
        preds_df = pd.DataFrame(preds)
    else:
        preds_df = preds.copy()

    datastore = get_datastore()

    mapping_sql = f"""
        SELECT meterid, utility
        FROM "{current_app.config['PARQUET_PATH']}"
        GROUP BY meterid, utility
    """

    mapping_rows = datastore.run_query(mapping_sql)
    mapping_df = pd.DataFrame(mapping_rows)

    # 🔑 FIX: normalize merge key types
    preds_df["meterid"] = preds_df["meterid"].astype(str)
    mapping_df["meterid"] = mapping_df["meterid"].astype(str)

    preds_df = preds_df.merge(mapping_df, on="meterid", how="left")

    return preds_df

def _cache_predict_all(datastore, preds_df):
    import pandas as pd

    # Drop old cache table completely)
    _ensure_predict_all_cache_table(datastore)

    # Normalize types
    preds_df = preds_df.copy()
    preds_df["meterid"] = preds_df["meterid"].astype(str)
    preds_df["horizon"] = preds_df["horizon"].astype(int)

    for col in ["as_of", "prediction_date"]:
        preds_df[col] = pd.to_datetime(preds_df[col], errors="coerce").dt.date

    for col in ["paymoney_pred", "energy_pred", "cash_pred"]:
        preds_df[col] = preds_df[col].astype(float)

    # Remove rows with missing critical keys
    preds_df = preds_df.dropna(subset=["meterid", "prediction_date", "horizon"])

    # Drop duplicates
    preds_df = preds_df.drop_duplicates(subset=["meterid", "prediction_date", "horizon"], keep="last")

    # Select only the columns in the table, in order
    insert_cols = ["meterid", "as_of", "horizon", "prediction_date",
                   "paymoney_pred", "energy_pred", "cash_pred", "utility"]

    # Convert all columns to native Python types in a vectorized way
    records = list(preds_df[insert_cols].where(preds_df[insert_cols].notna(), None).itertuples(index=False, name=None))

    # Insert into DuckDB
    datastore._con.executemany(
        "INSERT INTO predict_all_cache VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        records
    )

def _get_cached_predict_all_count(datastore, meterid: str | None = None, utilities: list[str] | None = None):
    _ensure_predict_all_cache_table(datastore)

    sql = "SELECT COUNT(*) FROM predict_all_cache"
    where_clauses = []
    params: list[object] = []

    if meterid is not None:
        where_clauses.append("meterid = ?")
        params.append(str(meterid))

    if utilities:
        placeholders = ", ".join("?" for _ in utilities)
        where_clauses.append(f"utility IN ({placeholders})")
        params.extend(utilities)

    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)

    row = datastore._con.execute(sql, params).fetchone()
    return row[0] if row else 0



def _get_cached_predict_all_monthly(datastore, meterid: str | None = None):
    _ensure_predict_all_cache_table(datastore)

    # Use meterid as string, since it's stored as VARCHAR in cache
    where_clause = "WHERE meterid = ?" if meterid is not None else ""
    params = [str(meterid)] if meterid is not None else []

    sql = f"""
        SELECT
            DATE_TRUNC('month', prediction_date)::DATE AS month,
            SUM(energy_pred)   AS energy_pred,
            SUM(paymoney_pred) AS paymoney_pred,
            SUM(cash_pred)     AS cash_pred
        FROM predict_all_cache
        {where_clause}
        GROUP BY month
        ORDER BY month
    """

    print(f"[DEBUG] _get_cached_predict_all_monthly SQL: {sql}")
    print(f"[DEBUG] _get_cached_predict_all_monthly params: {params}")

    rows = datastore._con.execute(sql, params).fetchall()
    print(f"[DEBUG] _get_cached_predict_all_monthly returned {len(rows)} rows")
    print(f"[DEBUG] Sample of first 5 rows: {rows[:5]}")

    # Keep the internal column names so legacy mapping works
    return [
        {
            "month": r[0].isoformat() if r[0] else None,
            "energy_pred": float(r[1] or 0),
            "paymoney_pred": float(r[2] or 0),
            "cash_pred": float(r[3] or 0),
        }
        for r in rows
    ]


def predictions_predict_all_cached():
    """
    Run predict_all, add location, cache into DuckDB, and return preview.
    """
    datastore = get_datastore()
    predictor = get_predictor()

    payload = request.get_json(silent=True) or {}

    try:
        preds_df = predictor.predict_all_from_db()
    except Exception:
        current_app.logger.exception("Predict All failed")
        return jsonify({"ok": False, "error": "Unable to generate predictions"}), 500

    # Add location
    preds_df = _add_location_to_predictions(preds_df)

    # Render preview table
    preview_rows = _get_cached_predict_all_preview(datastore)
    preview_html = _render_prediction_preview_table(preview_rows, PREVIEW_ROW_LIMIT)

    # Cache into DuckDB
    _cache_predict_all(datastore, preds_df, preview_html)
    selected_utilities = payload.get("utility", [])
    selected_meter = payload.get("meterid")

    row_count = _get_cached_predict_all_count(datastore, meterid=selected_meter, utilities=selected_utilities)


    return jsonify({
        "ok": True,
        "row_count": row_count,
        "preview_rows": min(PREVIEW_ROW_LIMIT, len(preds_df)),
        "preview_html": preview_html
    })
    
def _ensure_predict_all_cache_table(datastore):
    datastore._con.execute("""
        CREATE TABLE IF NOT EXISTS predict_all_cache (
            meterid           VARCHAR,        -- Unique meter identifier
            as_of             DATE,           -- Date the prediction is based on
            horizon           INT,            -- Months ahead (1-12)
            prediction_date   DATE,           -- Forecast month for this horizon
            paymoney_pred     DOUBLE,         -- Predicted paymoney
            energy_pred       DOUBLE,         -- Predicted energy consumption
            cash_pred         DOUBLE,         -- Predicted cash received
            utility           VARCHAR,        -- Optional: useful for filters
            PRIMARY KEY (meterid, as_of, prediction_date, horizon) -- ensures one row per meter per month
        );

    """)

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
bp.add_url_rule(
    "/predictions/api/predict-all-cache",
    view_func=predictions_predict_all_cached,
    methods=["POST"]
)
"""Dashboard blueprint and route handlers."""

from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import io
import pandas as pd
from flask import (
    Blueprint,
    Response,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from volta.utils.filter_params import FilterParams

bp = Blueprint("dashboard", __name__)

# Initial cap to avoid rendering tens of thousands of checkboxes on first load
DEFAULT_METERID_LIMIT = 500


def get_metrics():
    return current_app.extensions["metrics"]


def get_datastore():
    return current_app.extensions["datastore"]


def _parse_date(value: str) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        parsed = pd.to_datetime(value, errors="coerce")
        return parsed.date() if pd.notna(parsed) else None


def build_params(args, base_df: pd.DataFrame) -> FilterParams:
    """
    Build FilterParams from request args with case-insensitive column matching.
    Ensures selections like 'meterid' match DataFrame columns like 'MeterID'.
    """
    selections: Dict[str, List[str]] = {}
    exclude_cols = current_app.config["EXCLUDE_COLS"]

    # Map lowercase -> actual column name
    cols_lc = {str(c).lower(): c for c in base_df.columns}

    for column in base_df.columns:
        if column in exclude_cols:
            continue
        # Try exact match first, then lowercase alias
        values = args.getlist(column)
        if not values:
            values = args.getlist(str(column).lower())
        if values:
            selections[column] = [str(v) for v in values]

    # Also catch any args provided only in lowercase that didn't map above
    for key in args.keys():
        if key in selections:
            continue
        real_col = cols_lc.get(str(key).lower())
        if real_col and real_col not in exclude_cols:
            vals = args.getlist(key)
            if vals:
                selections[real_col] = [str(v) for v in vals]

    freq = (args.get("freq") or "D").upper()
    if freq not in ("D", "W", "M"):
        freq = "D"

    metric = args.get("metric") or None

    return FilterParams(
        start=_parse_date(args.get("start_date", "")),
        end=_parse_date(args.get("end_date", "")),
        selections=selections,
        freq=freq,
        metric=metric,
    )


def build_unique_values(df: pd.DataFrame, max_uniques: int = 200) -> Dict[str, List[str]]:
    unique: Dict[str, List[str]] = {}
    exclude_cols = current_app.config["EXCLUDE_COLS"]
    for column in df.columns:
        if column in exclude_cols:
            continue
        values = pd.Series(df[column].dropna().unique()).astype(str).tolist()
        values = sorted(set(values))[:max_uniques]
        unique[column] = values
    return unique


def get_base_date_bounds(df: pd.DataFrame) -> Tuple[str, str]:
    date_col = current_app.config["DATE_COL"]
    if date_col not in df.columns or len(df) == 0:
        return "", ""
    dmin = pd.to_datetime(df[date_col], errors="coerce").min()
    dmax = pd.to_datetime(df[date_col], errors="coerce").max()
    start = dmin.date().isoformat() if pd.notna(dmin) else ""
    end = dmax.date().isoformat() if pd.notna(dmax) else ""
    return start, end


def no_filters_selected(args, base_df: pd.DataFrame) -> bool:
    exclude_cols = current_app.config["EXCLUDE_COLS"]
    any_checkbox = any(
        args.getlist(column) for column in base_df.columns if column not in exclude_cols
    )
    if any_checkbox:
        return False
    base_min, base_max = get_base_date_bounds(base_df)
    start_in = args.get("start_date", "")
    end_in = args.get("end_date", "")
    if not start_in and not end_in:
        return True
    if (start_in == base_min or not start_in) and (end_in == base_max or not end_in):
        return True
    return False


@bp.route("/", methods=["GET"])
def index():
    date_col = current_app.config()["DATE_COL"] if callable(current_app.config) else current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()
    base = datastore.get(copy=True)

    if base.empty:
        return render_template("upload.html")

    if request.args and no_filters_selected(request.args, base):
        return redirect(url_for("dashboard.index"))

    params = build_params(request.args, base)
    after = params.apply(base, date_col)

    # Build defaults from filtered df for most facets
    unique_values = build_unique_values(after)

    # ---- Heavy facets ----
    meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)

    # Keep meterids capped (optionally from full table) to avoid UI lag on first load
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
            # Fall back to whatever was computed from `after`
            pass

    # IMPORTANT: LOC must reflect current filters (meterid/date/res_mapped...), not full table
    if "loc" in base.columns:
        try:
            clause, sql_params = FilterParams(
                start=params.start,
                end=params.end,
                selections=params.selections,
                freq=params.freq,
                metric=params.metric,
            ).to_sql_where(date_col=date_col, available_columns=base.columns)

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
            # If anything fails, leave whatever came from `after`
            pass
    # ---------------------------------------

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


@bp.route("/filters/options", methods=["POST"])
def filter_options():
    datastore = get_datastore()
    base = datastore.get(copy=False)
    if base.empty:
        return jsonify({"options": {}, "dates": {"min": "", "max": ""}, "rows": 0})

    payload = request.get_json(silent=True) or {}

    # --- Case-insensitive column mapping ---
    cols_lc = {str(c).lower(): c for c in base.columns}

    # Normalize selections (AND logic will be applied in FilterParams/SQL)
    raw_selections = payload.get("selections") or {}
    selections: Dict[str, List[str]] = {}
    for in_key, values in raw_selections.items():
        if not isinstance(values, (list, tuple)):
            continue
        real_col = cols_lc.get(str(in_key).lower())
        if not real_col:
            continue
        cleaned = [str(v) for v in values if v not in (None, "")]
        if cleaned:
            selections[real_col] = cleaned

    # Which facets to compute? (Compute only what the UI needs.)
    facets_in = payload.get("facets") or []
    if facets_in:
        facets = [
            cols_lc.get(str(f).lower())
            for f in facets_in
            if cols_lc.get(str(f).lower()) in base.columns
        ]
    else:
        # default: common facets if client didn't specify
        facets = [c for c in ["loc", "res_mapped", "meterid"] if c in base.columns]

    params = FilterParams(
        start=_parse_date(str(payload.get("start_date") or "")),
        end=_parse_date(str(payload.get("end_date") or "")),
        selections=selections,
        freq=(payload.get("freq") or "D").upper(),
        metric=payload.get("metric") or None,
    )

    date_col = current_app.config["DATE_COL"]
    clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=base.columns)

    # Helper: DISTINCT values for a column under the current WHERE
    def distinct(col: str) -> List[str]:
        df = datastore.run_query(
            f"""
            SELECT DISTINCT CAST({col} AS VARCHAR) AS v
            FROM prod.sales
            WHERE {clause} AND {col} IS NOT NULL
            ORDER BY v
            """,
            sql_params,
        )
        if df is None or df.empty:
            return []
        return df["v"].astype(str).tolist()

    # Build options only for the requested facets
    unique_values: Dict[str, List[str]] = {}
    for col in facets:
        unique_values[col] = distinct(col)

    # Keep meterid list capped (UI won’t choke)
    meter_cap = current_app.config.get("METERID_MAX_OPTIONS", DEFAULT_METERID_LIMIT)
    if "meterid" in unique_values:
        unique_values["meterid"] = unique_values["meterid"][: int(meter_cap)]

    # Min/max dates via SQL
    ddf = datastore.run_query(
        f"""
        SELECT
          MIN(CAST({date_col} AS DATE)) AS dmin,
          MAX(CAST({date_col} AS DATE)) AS dmax
        FROM prod.sales
        WHERE {clause}
        """,
        sql_params,
    )
    date_min = ddf.iloc[0]["dmin"].isoformat() if ddf is not None and pd.notna(ddf.iloc[0]["dmin"]) else ""
    date_max = ddf.iloc[0]["dmax"].isoformat() if ddf is not None and pd.notna(ddf.iloc[0]["dmax"]) else ""

    # Row count via SQL
    cdf = datastore.run_query(
        f"SELECT COUNT(*) AS n FROM prod.sales WHERE {clause};",
        sql_params,
    )
    rows = int(cdf.iloc[0]["n"]) if cdf is not None else 0

    return jsonify(
        {
            "options": unique_values,
            "dates": {"min": date_min, "max": date_max},
            "rows": rows,
        }
    )



@bp.route("/chart-data", methods=["GET"])
def chart_data():
    """Time-series for charts, computed in DuckDB (fast)."""
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()

    # Use full base for metric validation and to derive available columns
    base = datastore.get(copy=False)

    # Build params object
    params = build_params(request.args, base)

    requested_metrics = (params.metric or "").split(",")
    validated_metrics = [m for m in requested_metrics if metrics.validate(base, m)]

    if not validated_metrics:
        return jsonify(
            {"labels": [], "values": [], "metric_label": params.metric or "", "date_col": date_col}
        )

    # Build WHERE clause + params using FilterParams helper
    clause, sql_params = params.to_sql_where(
        date_col=date_col,
        available_columns=base.columns,
    )

    # Frequency mapping via helper (D/W/M -> day/week/month)
    trunc_unit = params.trunc_unit()

    # SQL aggregation for all metrics
    metric_sql = ", ".join([f"AVG({m}) AS {m}" for m in validated_metrics])
    sql = f"""
        SELECT
          date_trunc('{trunc_unit}', {date_col}) AS bucket,
          {metric_sql}
        FROM prod.sales
        WHERE {clause}
        GROUP BY 1
        ORDER BY 1;
    """

    df = datastore.run_query(sql, sql_params)

    if df is None or df.empty:
        return jsonify(
            {
                "labels": [],
                "values": {m: [] for m in validated_metrics},
                "metric_labels": {m: metrics.label(m) for m in validated_metrics},
                "date_col": date_col
            }
        )

    # Format labels per frequency
    def _fmt(ts: pd.Timestamp) -> str:
        if params.freq == "M":
            return pd.to_datetime(ts).strftime("%Y-%m")
        return pd.to_datetime(ts).date().isoformat()

    labels = [_fmt(v) for v in df["bucket"]]

    # Build values dictionary keyed by metric
    values_dict = {}
    for m in validated_metrics:
        values_dict[m] = [float(v) if pd.notna(v) else 0.0 for v in df[m]]

    return jsonify(
        {
            "labels": labels,
            "values": values_dict,
            "metric_labels": {m: metrics.label(m) for m in validated_metrics},
            "date_col": date_col
        }
    )


@bp.route("/pie-data", methods=["GET"])
def pie_data():
    # (kept as-is; still uses pandas for now)
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()
    base = datastore.get(copy=False)
    params = build_params(request.args, base)
    filtered = params.apply(base, date_col)

    metric = metrics.validate(filtered, params.metric)
    segment_col = (
        "res_mapped"
        if "res_mapped" in filtered.columns
        else ("loc" if "loc" in filtered.columns else None)
    )

    if not metric or segment_col is None or filtered.empty:
        return jsonify(
            {
                "labels": [],
                "values": [],
                "metric_label": params.metric or "",
                "segment": segment_col or "",
            }
        )

    series = filtered.dropna(subset=[segment_col]).copy()
    series[metric] = pd.to_numeric(series[metric], errors="coerce")
    series = series.dropna(subset=[metric])
    if series.empty:
        return jsonify(
            {
                "labels": [],
                "values": [],
                "metric_label": metrics.label(metric),
                "segment": segment_col,
            }
        )

    grp = series.groupby(series[segment_col].astype(str))[metric].sum().sort_values(
        ascending=False
    )

    top_n = 8
    if len(grp) > top_n:
        top = grp.iloc[:top_n]
        other_val = float(grp.iloc[top_n:].sum())
        labels = top.index.tolist() + ["Other"]
        values = [float(v) for v in top.values] + [other_val]
    else:
        labels = grp.index.tolist()
        values = [float(v) for v in grp.values]

    return jsonify(
        {
            "labels": labels,
            "values": values,
            "metric_label": metrics.label(metric),
            "segment": segment_col,
        }
    )


@bp.route("/bar-data", methods=["GET"])
def bar_data():
    # (kept as-is; still uses pandas for now)
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()
    base = datastore.get(copy=False)
    params = build_params(request.args, base)
    filtered = params.apply(base, date_col)

    metric = metrics.validate(filtered, params.metric)
    city_col = "loc"

    if not metric or city_col not in filtered.columns or filtered.empty:
        return jsonify(
            {
                "labels": [],
                "values": [],
                "metric_label": params.metric or "",
                "segment": city_col or "",
            }
        )

    series = filtered.dropna(subset=[city_col]).copy()
    series[metric] = pd.to_numeric(series[metric], errors="coerce")
    series = series.dropna(subset=[metric])
    if series.empty:
        return jsonify(
            {
                "labels": [],
                "values": [],
                "metric_label": metrics.label(metric),
                "segment": city_col,
            }
        )

    grp = series.groupby(series[city_col].astype(str))[metric].sum().sort_values(
        ascending=False
    )

    labels = grp.index.tolist()
    values = [float(v) for v in grp.values]
    return jsonify(
        {
            "labels": labels,
            "values": values,
            "metric_label": metrics.label(metric),
            "segment": city_col,
        }
    )


@bp.route("/download-csv", methods=["GET"])
def download_csv():
    """Download the entire filtered dataset as CSV."""
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    base = datastore.get(copy=False)

    params = build_params(request.args, base)
    filtered = params.apply(base, date_col)

    buf = io.StringIO()
    filtered.to_csv(buf, index=False)
    buf.seek(0)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"export_{ts}.csv"

    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename={filename}'},
    )


@bp.route("/health", methods=["GET"])
def health():
    datastore = get_datastore()
    try:
        df = datastore.get(copy=False)
        return (
            jsonify(
                {
                    "ok": True,
                    "rows": int(len(df)),
                    "cols": int(len(df.columns)),
                }
            ),
            200,
        )
    except Exception as exc:  # pragma: no cover - defensive logging path
        current_app.logger.exception("Healthcheck failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


# --- New: on-demand meterid options endpoint (for search-as-you-type) ---
@bp.route("/options/meterid", methods=["GET", "POST"])
def options_meterid():
    """
    Returns up to 200 distinct meterid values, optionally filtered by:
      - q: substring match (case-insensitive)
      - other filters (loc, res_mapped, etc.) from selections/dates
    """
    datastore = get_datastore()

    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        q = str(payload.get("q") or "").strip()
        try:
            limit = int(payload.get("limit") or 200)
        except (TypeError, ValueError):
            limit = 200
        limit = max(limit, 1)

        base = datastore.get(copy=False)
        if base.empty or "meterid" not in base.columns:
            return jsonify([])

        raw_selections = payload.get("selections") or {}
        selections: Dict[str, List[str]] = {}

        # Case-insensitive mapping for non-meterid facets
        cols_lc = {str(c).lower(): c for c in base.columns}
        meterid_real = cols_lc.get("meterid", "meterid")

        for in_key, values in raw_selections.items():
            if not isinstance(values, (list, tuple)):
                continue
            real_col = cols_lc.get(str(in_key).lower())
            if not real_col:
                continue
            # Skip filtering the meterid list by itself
            if str(real_col).lower() == "meterid":
                continue
            cleaned = [str(v) for v in values if v not in (None, "")]
            if cleaned:
                selections[real_col] = cleaned

        params = FilterParams(
            start=_parse_date(str(payload.get("start_date") or "")),
            end=_parse_date(str(payload.get("end_date") or "")),
            selections=selections,
        )

        date_col = current_app.config["DATE_COL"]
        filtered = params.apply(base, date_col)
        if filtered.empty or meterid_real not in filtered.columns:
            return jsonify([])

        series = filtered[meterid_real].dropna().astype(str)
        if q:
            series = series[series.str.contains(q, case=False, na=False)]

        unique_values = sorted(set(series.tolist()))
        return jsonify(unique_values[:limit])

    # GET fallback (unchanged)
    q = (request.args.get("q") or "").strip()
    loc = request.args.get("loc")
    try:
        limit = int(request.args.get("limit") or 200)
    except (TypeError, ValueError):
        limit = 200
    limit = max(limit, 1)

    sql = """
        SELECT DISTINCT meterid AS v
        FROM prod.sales
        WHERE meterid IS NOT NULL
    """
    params = []
    if loc:
        sql += " AND CAST(loc AS VARCHAR) = ?"
        params.append(loc)
    if q:
        sql += " AND CAST(meterid AS VARCHAR) ILIKE '%' || ? || '%'"
        params.append(q)

    sql += " ORDER BY v LIMIT ?"
    params.append(limit)

    rows = datastore.run_query(sql, params)
    return jsonify(rows["v"].astype(str).tolist())


__all__ = ["bp"]

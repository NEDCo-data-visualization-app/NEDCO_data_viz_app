from flask import Flask, render_template, request, redirect, url_for, jsonify
import pandas as pd
import os
import math

app = Flask(__name__)

DATA_PATH = "data/wkfile_shiny.parquet"
DATE_COL = "chargedate"

# Hide these from the checkbox UI
EXCLUDE_COLS = {"chargedate","chargedate_str","month","month_str","year","kwh","ghc","paymoney","res"}

RES_MAP = {"N-Resid [0]": "Commercial", "Resid [1]": "Residential"}

# in-memory cache
_DF = None

# chartable numeric metrics (label for UI)
CHART_METRICS = [("kwh", "kWh"), ("paymoney", "Pay"), ("ghc", "GHC")]

def available_chart_metrics(df):
    return [(c, lbl) for c, lbl in CHART_METRICS if c in df.columns]

def clean_nan_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove all rows that contain any NaN values.
    Returns a new DataFrame without modifying the original.
    """
    return df.dropna(how="any").reset_index(drop=True)


def load_df():
    """Load parquet once, do one-time preprocessing, keep cached in memory."""
    global _DF
    if _DF is None:
        df = pd.read_parquet(DATA_PATH)
        df = clean_nan_rows(df)
        # One-time mapping
        if "res" in df.columns:
            df["res_mapped"] = df["res"].astype(str).map(RES_MAP).fillna("Unknown")

        # One-time date parsing
        if DATE_COL in df.columns and not pd.api.types.is_datetime64_any_dtype(df[DATE_COL]):
            df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")

        # One-time numeric coercion so stats/charts work
        for numcol in ("kwh", "paymoney", "ghc"):
            if numcol in df.columns:
                df[numcol] = pd.to_numeric(df[numcol], errors="coerce")

        _DF = df
    return _DF

def apply_checkbox_filters(df: pd.DataFrame, args) -> pd.DataFrame:
    out = df
    for c in out.columns:
        if c in EXCLUDE_COLS:
            continue
        sel = args.getlist(c)
        if sel:
            out = out[out[c].astype(str).isin(sel)]
    return out

def apply_date_filter(df: pd.DataFrame, args) -> pd.DataFrame:
    """Apply date filter if start/end provided."""
    if DATE_COL not in df.columns:
        return df
    start_date = args.get("start_date", "")
    end_date   = args.get("end_date", "")
    out = df
    if start_date:
        out = out[out[DATE_COL] >= pd.to_datetime(start_date)]
    if end_date:
        out = out[out[DATE_COL] <= pd.to_datetime(end_date)]
    return out

def build_unique_values(df: pd.DataFrame, max_uniques=200) -> dict:
    unique = {}
    for c in df.columns:
        if c in EXCLUDE_COLS:
            continue
        vals = pd.Series(df[c].dropna().unique()).astype(str).tolist()
        vals = sorted(set(vals))[:max_uniques]
        unique[c] = vals
    return unique

def get_base_date_bounds(df: pd.DataFrame) -> tuple[str, str]:
    if DATE_COL not in df.columns or len(df) == 0:
        return "", ""
    dmin = pd.to_datetime(df[DATE_COL], errors="coerce").min()
    dmax = pd.to_datetime(df[DATE_COL], errors="coerce").max()
    s = dmin.date().isoformat() if pd.notna(dmin) else ""
    e = dmax.date().isoformat() if pd.notna(dmax) else ""
    return s, e

def no_filters_selected(args, base_df: pd.DataFrame) -> bool:
    any_checkbox = any(args.getlist(c) for c in base_df.columns if c not in EXCLUDE_COLS)
    if any_checkbox:
        return False
    base_min, base_max = get_base_date_bounds(base_df)
    start_in = args.get("start_date", "")
    end_in   = args.get("end_date", "")
    if not start_in and not end_in:
        return True
    if (start_in == base_min or not start_in) and (end_in == base_max or not end_in):
        return True
    return False

# Stats blocks you added
NUMERIC_KPI_COLS = [("kwh", "kWh"), ("paymoney", "Pay"), ("ghc", "GHC")]

def compute_stats(df: pd.DataFrame) -> dict:
    stats = {}
    for col, label in NUMERIC_KPI_COLS:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(s) > 0:
                stats[col] = {
                    "label": label,
                    "mean": float(s.mean()),
                    "median": float(s.median()),
                    "min": float(s.min()),
                    "max": float(s.max()),
                }
    return stats

def compute_summary(df: pd.DataFrame) -> dict:
    out = {
        "rows": len(df),
        "cols": len(df.columns),
        "meters": (df["meterid"].nunique() if "meterid" in df.columns else None),
        "locations": (df["loc"].nunique() if "loc" in df.columns else None),
        "date_min": "", "date_max": ""
    }
    if DATE_COL in df.columns and len(df) > 0:
        dmin = pd.to_datetime(df[DATE_COL], errors="coerce").min()
        dmax = pd.to_datetime(df[DATE_COL], errors="coerce").max()
        if pd.notna(dmin): out["date_min"] = dmin.date().isoformat()
        if pd.notna(dmax): out["date_max"] = dmax.date().isoformat()
    return out

@app.route("/", methods=["GET"])
def index():
    base = load_df().copy()

    # Reset behavior when nothing is actually selected
    if request.args and no_filters_selected(request.args, base):
        return redirect(url_for("index"))

    # Filters
    after_cat  = apply_checkbox_filters(base, request.args)
    after_date = apply_date_filter(after_cat, request.args)

    # Unique options cascade with final dataset
    unique_values = build_unique_values(after_date)

    # Date inputs show min/max of final dataset
    start_value = end_value = ""
    if DATE_COL in after_date.columns and len(after_date) > 0:
        dmin = pd.to_datetime(after_date[DATE_COL], errors="coerce").min()
        dmax = pd.to_datetime(after_date[DATE_COL], errors="coerce").max()
        if pd.notna(dmin): start_value = dmin.date().isoformat()
        if pd.notna(dmax): end_value   = dmax.date().isoformat()

    # Stats + summary
    stats = compute_stats(after_date)
    summary = compute_summary(after_date)

    # Chart metric options (only those present)
    chart_metrics = available_chart_metrics(after_date)
    default_metric = chart_metrics[0][0] if chart_metrics else ""

    # Preview: first 10 rows only
    preview_html = after_date.head(10).to_html(
        classes="table table-sm table-striped table-hover", index=False
    )

    return render_template(
        "index.html",
        date_col=DATE_COL,
        stats=stats,
        summary=summary,
        start_value=start_value,
        end_value=end_value,
        unique_values=unique_values,
        args=request.args,
        total_rows=len(after_date),
        total_cols=len(after_date.columns),
        preview_html=preview_html,
        chart_metrics=chart_metrics,
        default_metric=default_metric,
    )

@app.route("/chart-data", methods=["GET"])
def chart_data():
    """Return JSON (date-bucket, mean(metric)) for current filters with chosen granularity.
       NaN/Inf values are converted to JSON null so Chart.js can render gaps safely.
    """
    # use cached DF
    base = _DF if _DF is not None else load_df()
    base = base.copy(deep=False)

    # Apply same filters as the table
    after_cat  = apply_checkbox_filters(base, request.args)
    filtered   = apply_date_filter(after_cat, request.args)

    metric = request.args.get("metric", "")
    if not metric or metric not in filtered.columns or DATE_COL not in filtered.columns or filtered.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metric, "date_col": DATE_COL})

    # Parse dates & keep valid rows
    s = filtered.dropna(subset=[DATE_COL]).copy()
    s[DATE_COL] = pd.to_datetime(s[DATE_COL], errors="coerce")
    s = s.dropna(subset=[DATE_COL])
    if s.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metric, "date_col": DATE_COL})

    # Frequency handling
    freq = (request.args.get("freq", "D") or "D").upper()
    if freq not in ("D", "W", "M"):
        freq = "D"
    freq_map = {"D": "D", "W": "W-MON", "M": "M"}  # weekly anchored to Monday

    # Resample to mean
    ts = s.set_index(s[DATE_COL])
    grp = ts[metric].resample(freq_map[freq], label="left", closed="left").mean().sort_index()

    # If no buckets, bail out
    if grp is None or len(grp) == 0:
        return jsonify({"labels": [], "values": [], "metric_label": metric, "date_col": DATE_COL})

    # Labels
    if freq == "M":
        labels = [idx.strftime("%Y-%m") for idx in grp.index]
    else:
        labels = [idx.date().isoformat() for idx in grp.index]

    # Values: convert NaN/Inf -> None (JSON null)
    vals = grp.tolist()
    values = []
    for v in vals:
        if v is None:
            values.append(None)
        elif isinstance(v, float):
            if not math.isfinite(v):  # NaN, +Inf, -Inf
                values.append(None)
            else:
                values.append(float(v))
        else:
            # try to coerce any stray types
            try:
                values.append(float(v))
            except Exception:
                values.append(None)

    # If all nulls, return empty arrays to trigger "no data" UI
    if not any(v is not None for v in values):
        return jsonify({"labels": [], "values": [], "metric_label": dict(CHART_METRICS).get(metric, metric), "date_col": DATE_COL})

    metric_label = dict(CHART_METRICS).get(metric, metric)
    return jsonify({"labels": labels, "values": values, "metric_label": metric_label, "date_col": DATE_COL})


@app.route("/pie-data", methods=["GET"])
def pie_data():
    """
    Return JSON for a composition donut: sum(metric) by segment.
    Default segment is 'res_mapped' (Commercial/Residential). Falls back to 'loc' if needed.
    """
    base = _DF if _DF is not None else load_df()
    base = base.copy(deep=False)

    after_cat  = apply_checkbox_filters(base, request.args)
    filtered   = apply_date_filter(after_cat, request.args)

    metric = request.args.get("metric", "")
    segment_col = "res_mapped" if "res_mapped" in filtered.columns else ("loc" if "loc" in filtered.columns else None)

    if not metric or metric not in filtered.columns or segment_col is None or filtered.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metric, "segment": segment_col or ""})

    # keep valid metric rows
    s = filtered.dropna(subset=[segment_col]).copy()
    s[metric] = pd.to_numeric(s[metric], errors="coerce")
    s = s.dropna(subset=[metric])
    if s.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metric, "segment": segment_col})

    # Sum by segment
    grp = s.groupby(s[segment_col].astype(str))[metric].sum().sort_values(ascending=False)

    # Optional: top-N + "Other"
    TOP_N = 8
    if len(grp) > TOP_N:
        top = grp.iloc[:TOP_N]
        other_val = float(grp.iloc[TOP_N:].sum())
        labels = top.index.tolist() + ["Other"]
        values = [float(v) for v in top.values] + [other_val]
    else:
        labels = grp.index.tolist()
        values = [float(v) for v in grp.values]

    metric_label = dict(CHART_METRICS).get(metric, metric)
    return jsonify({
        "labels": labels,
        "values": values,
        "metric_label": metric_label,
        "segment": segment_col
    })


@app.route("/bar-data", methods=["GET"])
def bar_data():
    """
    Return JSON for a bar chart: sum(metric) grouped by city (loc).
    """
    base = _DF if _DF is not None else load_df()
    base = base.copy(deep=False)

    after_cat  = apply_checkbox_filters(base, request.args)
    filtered   = apply_date_filter(after_cat, request.args)

    metric = request.args.get("metric", "")
    city_col = "loc"  # adjust if your city column has a different name

    if not metric or metric not in filtered.columns or city_col not in filtered.columns or filtered.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metric, "segment": city_col or ""})

    s = filtered.dropna(subset=[city_col]).copy()
    s[metric] = pd.to_numeric(s[metric], errors="coerce")
    s = s.dropna(subset=[metric])
    if s.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metric, "segment": city_col})

    grp = s.groupby(s[city_col].astype(str))[metric].sum().sort_values(ascending=False)

    labels = grp.index.tolist()
    values = [float(v) for v in grp.values]

    metric_label = dict(CHART_METRICS).get(metric, metric)
    return jsonify({
        "labels": labels,
        "values": values,
        "metric_label": metric_label,
        "segment": city_col
    })





if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)

from flask import Flask, render_template, request, redirect, url_for, jsonify
import pandas as pd
from functools import lru_cache
from typing import Dict, List, Tuple, Union
from datetime import datetime, date
from typing import Optional
from filter_params import FilterParams


app = Flask(__name__)

DATA_PATH = "data/wkfile_shiny.parquet"
DATE_COL = "chargedate"

# Hide these from the checkbox UI
EXCLUDE_COLS = {
    "chargedate", "chargedate_str", "month", "month_str", "year",
    "kwh", "ghc", "paymoney", "res"
}

RES_MAP = {"N-Resid [0]": "Commercial", "Resid [1]": "Residential"}

# chartable numeric metrics (label for UI)
CHART_METRICS: List[Tuple[str, str]] = [("kwh", "kWh"), ("paymoney", "Pay"), ("ghc", "GHC")]


def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    # Prefer strict YYYY-MM-DD, fall back to pandas parsing
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        d = pd.to_datetime(s, errors="coerce")
        return d.date() if pd.notna(d) else None

def build_params(args, base_df: pd.DataFrame) -> FilterParams:
    # collect categorical selections for every non-excluded column
    selections: Dict[str, List[str]] = {}
    for c in base_df.columns:
        if c in EXCLUDE_COLS:
            continue
        vals = args.getlist(c)
        if vals:
            selections[c] = [str(v) for v in vals]

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

def apply_filters(df: pd.DataFrame, p: FilterParams) -> pd.DataFrame:
    out = df
    # categorical filters
    for col, vals in p.selections.items():
        if col in out.columns and vals:
            out = out[out[col].astype(str).isin(vals)]
    # date range
    if DATE_COL in out.columns:
        if p.start is not None:
            out = out[out[DATE_COL] >= pd.Timestamp(p.start)]
        if p.end is not None:
            out = out[out[DATE_COL] <= pd.Timestamp(p.end)]
    return out



def available_chart_metrics(df: pd.DataFrame) -> List[Tuple[str, str]]:
    return [(c, lbl) for c, lbl in CHART_METRICS if c in df.columns]


def clean_nan_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove all rows that contain any NaN values.
    Returns a new DataFrame without modifying the original.
    """
    return df.dropna(how="any").reset_index(drop=True)


@lru_cache(maxsize=1)
def load_df() -> pd.DataFrame:
    """Load parquet once, do one-time preprocessing, cache result in-memory."""
    df = pd.read_parquet(DATA_PATH)

    # Drop duplicate rows (keep first occurrence)
    df = df.drop_duplicates().reset_index(drop=True)

    # Remove NaN rows
    df = clean_nan_rows(df)

    # One-time mapping
    if "res" in df.columns:
        df["res_mapped"] = df["res"].astype(str).map(RES_MAP).fillna("Unknown")

    # One-time date parsing
    if DATE_COL in df.columns and not pd.api.types.is_datetime64_any_dtype(df[DATE_COL]):
        # adjust format if needed
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce", format="%d-%b-%y")

    # One-time numeric coercion so stats/charts work
    for numcol in ("kwh", "paymoney", "ghc"):
        if numcol in df.columns:
            df[numcol] = pd.to_numeric(df[numcol], errors="coerce")

    return df



def reload_df() -> None:
    """Clear the cached DataFrame (use if file changes)."""
    load_df.cache_clear()


def build_unique_values(df: pd.DataFrame, max_uniques: int = 200) -> Dict[str, List[str]]:
    unique: Dict[str, List[str]] = {}
    for c in df.columns:
        if c in EXCLUDE_COLS:
            continue
        vals = pd.Series(df[c].dropna().unique()).astype(str).tolist()
        vals = sorted(set(vals))[:max_uniques]
        unique[c] = vals
    return unique


def get_base_date_bounds(df: pd.DataFrame) -> Tuple[str, str]:
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
    end_in = args.get("end_date", "")
    if not start_in and not end_in:
        return True
    if (start_in == base_min or not start_in) and (end_in == base_max or not end_in):
        return True
    return False


# Stats blocks you added
NUMERIC_KPI_COLS: List[Tuple[str, str]] = [("kwh", "kWh"), ("paymoney", "Pay"), ("ghc", "GHC")]


def compute_stats(df: pd.DataFrame) -> Dict[str, Dict[str, Union[float, str]]]:
    stats: Dict[str, Dict[str, Union[float, str]]] = {}
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


def compute_summary(df: pd.DataFrame) -> Dict[str, Union[int, str, None]]:
    out: Dict[str, Union[int, str, None]] = {
        "rows": len(df),
        "cols": len(df.columns),
        "meters": (df["meterid"].nunique() if "meterid" in df.columns else None),
        "locations": (df["loc"].nunique() if "loc" in df.columns else None),
        "date_min": "",
        "date_max": "",
    }
    if DATE_COL in df.columns and len(df) > 0:
        dmin = pd.to_datetime(df[DATE_COL], errors="coerce").min()
        dmax = pd.to_datetime(df[DATE_COL], errors="coerce").max()
        if pd.notna(dmin):
            out["date_min"] = dmin.date().isoformat()
        if pd.notna(dmax):
            out["date_max"] = dmax.date().isoformat()
    return out


@app.route("/", methods=["GET"])
def index():
    base = load_df().copy()

    # keep your "reset" behavior
    if request.args and no_filters_selected(request.args, base):
        return redirect(url_for("index"))

    # NEW: build once, filter once
    params = build_params(request.args, base)
    after = params.apply(base, DATE_COL)

    unique_values = build_unique_values(after)

    start_value = end_value = ""
    if DATE_COL in after.columns and len(after) > 0:
        dmin = pd.to_datetime(after[DATE_COL], errors="coerce").min()
        dmax = pd.to_datetime(after[DATE_COL], errors="coerce").max()
        if pd.notna(dmin): start_value = dmin.date().isoformat()
        if pd.notna(dmax): end_value   = dmax.date().isoformat()

    stats = compute_stats(after)
    summary = compute_summary(after)

    chart_metrics = available_chart_metrics(after)
    default_metric = chart_metrics[0][0] if chart_metrics else ""

    preview_html = after.head(10).to_html(
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
        total_rows=len(after),
        total_cols=len(after.columns),
        preview_html=preview_html,
        chart_metrics=chart_metrics,
        default_metric=default_metric,
    )



@app.route("/chart-data", methods=["GET"])
def chart_data():
    base = load_df().copy(deep=False)
    params = build_params(request.args, base)
    filtered = params.apply(base, DATE_COL)

    metric = params.metric or ""
    if not metric or metric not in filtered.columns or DATE_COL not in filtered.columns or filtered.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metric, "date_col": DATE_COL})

    s = filtered.dropna(subset=[DATE_COL]).copy()
    s[DATE_COL] = pd.to_datetime(s[DATE_COL], errors="coerce")
    s = s.dropna(subset=[DATE_COL])
    if s.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metric, "date_col": DATE_COL})

    rule = {"D": "D", "W": "W-MON", "M": "M"}[params.freq]

    ts = s.set_index(s[DATE_COL])
    grp = (ts[metric]
           .resample(rule, label="left", closed="left")
           .mean()
           .dropna()
           .sort_index())

    if grp is None or len(grp) == 0:
        return jsonify({"labels": [], "values": [], "metric_label": dict(CHART_METRICS).get(metric, metric), "date_col": DATE_COL})

    labels = [idx.strftime("%Y-%m") if params.freq == "M" else idx.date().isoformat() for idx in grp.index]
    values = [float(v) for v in grp.values]
    metric_label = dict(CHART_METRICS).get(metric, metric)
    return jsonify({"labels": labels, "values": values, "metric_label": metric_label, "date_col": DATE_COL})



@app.route("/pie-data", methods=["GET"])
def pie_data():
    base = load_df().copy(deep=False)
    params = build_params(request.args, base)
    filtered = params.apply(base, DATE_COL)

    metric = params.metric or ""
    segment_col = "res_mapped" if "res_mapped" in filtered.columns else ("loc" if "loc" in filtered.columns else None)

    if not metric or metric not in filtered.columns or segment_col is None or filtered.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metric, "segment": segment_col or ""})

    s = filtered.dropna(subset=[segment_col]).copy()
    s[metric] = pd.to_numeric(s[metric], errors="coerce")
    s = s.dropna(subset=[metric])
    if s.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metric, "segment": segment_col})

    grp = s.groupby(s[segment_col].astype(str))[metric].sum().sort_values(ascending=False)

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
    return jsonify({"labels": labels, "values": values, "metric_label": metric_label, "segment": segment_col})



@app.route("/bar-data", methods=["GET"])
def bar_data():
    base = load_df().copy(deep=False)
    params = build_params(request.args, base)
    filtered = params.apply(base, DATE_COL)

    metric = params.metric or ""
    city_col = "loc"

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
    return jsonify({"labels": labels, "values": values, "metric_label": metric_label, "segment": city_col})



if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)

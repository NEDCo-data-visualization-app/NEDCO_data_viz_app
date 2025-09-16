from flask import Flask, render_template, request, redirect, url_for, jsonify
import os
import logging
import pandas as pd
from typing import Dict, List, Tuple, Union, Optional
from datetime import datetime, date
from filter_params import FilterParams

# ------------------------------ Logging --------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("volta")
# -----------------------------------------------------------------------------

app = Flask(__name__)

# ------------------------------ Config ---------------------------------------
class Config:
    # You can override these with environment variables
    DATA_PATH = os.getenv("VOLTA_DATA_PATH", "data/wkfile_shiny.parquet")
    DATE_COL = os.getenv("VOLTA_DATE_COL", "chargedate")

    # Hide these from the checkbox UI
    EXCLUDE_COLS = {
        "chargedate", "chargedate_str", "month", "month_str", "year",
        "kwh", "ghc", "paymoney", "res"
    }

    RES_MAP = {"N-Resid [0]": "Commercial", "Resid [1]": "Residential"}

    # Centralized metrics & frequency config
    METRICS: Dict[str, str] = {
        "kwh": "kWh",
        "paymoney": "Pay",
        "ghc": "GHC",
    }

    FREQ_RULE: Dict[str, str] = {
        "D": "D",
        "W": "W-MON",  # weekly anchored to Monday
        "M": "M",
    }

app.config.from_object(Config)
# -----------------------------------------------------------------------------

# ------------------------------ OOP: Metrics ---------------------------------
class Metrics:
    """Encapsulate metric mapping + helpers."""
    def __init__(self, mapping: Dict[str, str]):
        self.mapping = dict(mapping)

    def label(self, key: Optional[str]) -> str:
        if not key:
            return ""
        return self.mapping.get(key, key)

    def validate(self, df: pd.DataFrame, metric: Optional[str]) -> Optional[str]:
        if not metric:
            return None
        return metric if (metric in df.columns and metric in self.mapping) else None

    def available(self, df: pd.DataFrame) -> List[Tuple[str, str]]:
        return [(k, v) for k, v in self.mapping.items() if k in df.columns]

metrics = Metrics(app.config["METRICS"])
# -----------------------------------------------------------------------------

def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        d = pd.to_datetime(s, errors="coerce")
        return d.date() if pd.notna(d) else None

def build_params(args, base_df: pd.DataFrame) -> FilterParams:
    # collect categorical selections for every non-excluded column (including meterid)
    selections: Dict[str, List[str]] = {}
    for c in base_df.columns:
        if c in app.config["EXCLUDE_COLS"]:
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

def clean_nan_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(how="any").reset_index(drop=True)

# ------------------------------ OOP: DataStore -------------------------------
class DataStore:
    """Own data loading, preprocessing, derived stats, and in-memory caching."""
    def __init__(self, app: Flask):
        self.app = app
        self._df: Optional[pd.DataFrame] = None

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop_duplicates().reset_index(drop=True)
        df = clean_nan_rows(df)

        if "res" in df.columns:
            df["res_mapped"] = df["res"].astype(str).map(self.app.config["RES_MAP"]).fillna("Unknown")

        date_col = self.app.config["DATE_COL"]
        if date_col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce", format="%d-%b-%y")

        for numcol in metrics.mapping.keys():
            if numcol in df.columns:
                df[numcol] = pd.to_numeric(df[numcol], errors="coerce")

        return df

    def load(self) -> pd.DataFrame:
        if self._df is not None:
            return self._df

        path = self.app.config["DATA_PATH"]
        if not os.path.exists(path):
            logger.error("DATA_PATH not found: %s", path)
            raise FileNotFoundError(
                f"DATA_PATH not found: {path}. Set VOLTA_DATA_PATH to override."
            )

        try:
            raw = pd.read_parquet(path)
        except Exception:
            logger.exception("Failed to read parquet at %s", path)
            raise

        logger.info("Loaded raw DataFrame: %s rows, %s cols", len(raw), len(raw.columns))
        self._df = self._preprocess(raw)
        logger.info("Processed DataFrame: %s rows, %s cols", len(self._df), len(self._df.columns))
        return self._df

    def get(self, copy: bool = True) -> pd.DataFrame:
        df = self.load()
        return df.copy(deep=False) if copy else df

    def reload(self) -> None:
        self._df = None
        logger.info("DataStore cache cleared")

    def compute_stats(self, df: pd.DataFrame) -> Dict[str, Dict[str, Union[float, str]]]:
        stats: Dict[str, Dict[str, Union[float, str]]] = {}
        for key, label in metrics.mapping.items():
            if key in df.columns:
                s = pd.to_numeric(df[key], errors="coerce").dropna()
                if len(s) > 0:
                    stats[key] = {
                        "label": label,
                        "mean": float(s.mean()),
                        "median": float(s.median()),
                        "min": float(s.min()),
                        "max": float(s.max()),
                    }
        return stats

    def compute_summary(self, df: pd.DataFrame) -> Dict[str, Union[int, str, None]]:
        date_col = self.app.config["DATE_COL"]
        out: Dict[str, Union[int, str, None]] = {
            "rows": len(df),
            "cols": len(df.columns),
            "meters": (df["meterid"].nunique() if "meterid" in df.columns else None),
            "locations": (df["loc"].nunique() if "loc" in df.columns else None),
            "date_min": "",
            "date_max": "",
        }
        if date_col in df.columns and len(df) > 0:
            dmin = pd.to_datetime(df[date_col], errors="coerce").min()
            dmax = pd.to_datetime(df[date_col], errors="coerce").max()
            if pd.notna(dmin):
                out["date_min"] = dmin.date().isoformat()
            if pd.notna(dmax):
                out["date_max"] = dmax.date().isoformat()
        return out

datastore = DataStore(app)
# -----------------------------------------------------------------------------

def load_df() -> pd.DataFrame:
    return datastore.load()

def reload_df() -> None:
    datastore.reload()

def build_unique_values(df: pd.DataFrame, max_uniques: int = 200) -> Dict[str, List[str]]:
    unique: Dict[str, List[str]] = {}
    for c in df.columns:
        if c in app.config["EXCLUDE_COLS"]:
            continue
        vals = pd.Series(df[c].dropna().unique()).astype(str).tolist()
        vals = sorted(set(vals))[:max_uniques]
        unique[c] = vals
    return unique

def get_base_date_bounds(df: pd.DataFrame) -> Tuple[str, str]:
    date_col = app.config["DATE_COL"]
    if date_col not in df.columns or len(df) == 0:
        return "", ""
    dmin = pd.to_datetime(df[date_col], errors="coerce").min()
    dmax = pd.to_datetime(df[date_col], errors="coerce").max()
    s = dmin.date().isoformat() if pd.notna(dmin) else ""
    e = dmax.date().isoformat() if pd.notna(dmax) else ""
    return s, e

def no_filters_selected(args, base_df: pd.DataFrame) -> bool:
    any_checkbox = any(args.getlist(c) for c in base_df.columns if c not in app.config["EXCLUDE_COLS"])
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

# --------------------------------- Routes ------------------------------------

@app.route("/", methods=["GET"])
def index():
    date_col = app.config["DATE_COL"]
    base = load_df().copy()

    if request.args and no_filters_selected(request.args, base):
        return redirect(url_for("index"))

    params = build_params(request.args, base)
    after = params.apply(base, date_col)

    unique_values = build_unique_values(after)

    start_value = end_value = ""
    if date_col in after.columns and len(after) > 0:
        dmin = pd.to_datetime(after[date_col], errors="coerce").min()
        dmax = pd.to_datetime(after[date_col], errors="coerce").max()
        if pd.notna(dmin): start_value = dmin.date().isoformat()
        if pd.notna(dmax): end_value   = dmax.date().isoformat()

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

@app.route("/chart-data", methods=["GET"])
def chart_data():
    date_col = app.config["DATE_COL"]
    base = datastore.get(copy=False)
    params = build_params(request.args, base)
    filtered = params.apply(base, date_col)

    metric = metrics.validate(filtered, params.metric)
    if not metric or date_col not in filtered.columns or filtered.empty:
        return jsonify({"labels": [], "values": [], "metric_label": params.metric or "", "date_col": date_col})

    s = filtered.dropna(subset=[date_col]).copy()
    s[date_col] = pd.to_datetime(s[date_col], errors="coerce")
    s = s.dropna(subset=[date_col])
    if s.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metrics.label(metric), "date_col": date_col})

    rule = app.config["FREQ_RULE"].get(params.freq, "D")

    ts = s.set_index(s[date_col])
    grp = (ts[metric]
           .resample(rule, label="left", closed="left")
           .mean()
           .dropna()
           .sort_index())

    if grp is None or len(grp) == 0:
        return jsonify({"labels": [], "values": [], "metric_label": metrics.label(metric), "date_col": date_col})

    labels = [idx.strftime("%Y-%m") if params.freq == "M" else idx.date().isoformat() for idx in grp.index]
    values = [float(v) for v in grp.values]
    return jsonify({"labels": labels, "values": values, "metric_label": metrics.label(metric), "date_col": date_col})

@app.route("/pie-data", methods=["GET"])
def pie_data():
    date_col = app.config["DATE_COL"]
    base = datastore.get(copy=False)
    params = build_params(request.args, base)
    filtered = params.apply(base, date_col)

    metric = metrics.validate(filtered, params.metric)
    segment_col = "res_mapped" if "res_mapped" in filtered.columns else ("loc" if "loc" in filtered.columns else None)

    if not metric or segment_col is None or filtered.empty:
        return jsonify({"labels": [], "values": [], "metric_label": params.metric or "", "segment": segment_col or ""})

    s = filtered.dropna(subset=[segment_col]).copy()
    s[metric] = pd.to_numeric(s[metric], errors="coerce")
    s = s.dropna(subset=[metric])
    if s.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metrics.label(metric), "segment": segment_col})

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

    return jsonify({"labels": labels, "values": values, "metric_label": metrics.label(metric), "segment": segment_col})

@app.route("/bar-data", methods=["GET"])
def bar_data():
    date_col = app.config["DATE_COL"]
    base = datastore.get(copy=False)
    params = build_params(request.args, base)
    filtered = params.apply(base, date_col)

    metric = metrics.validate(filtered, params.metric)
    city_col = "loc"

    if not metric or city_col not in filtered.columns or filtered.empty:
        return jsonify({"labels": [], "values": [], "metric_label": params.metric or "", "segment": city_col or ""})

    s = filtered.dropna(subset=[city_col]).copy()
    s[metric] = pd.to_numeric(s[metric], errors="coerce")
    s = s.dropna(subset=[metric])
    if s.empty:
        return jsonify({"labels": [], "values": [], "metric_label": metrics.label(metric), "segment": city_col})

    grp = s.groupby(s[city_col].astype(str))[metric].sum().sort_values(ascending=False)

    labels = grp.index.tolist()
    values = [float(v) for v in grp.values]
    return jsonify({"labels": labels, "values": values, "metric_label": metrics.label(metric), "segment": city_col})

@app.route("/health", methods=["GET"])
def health():
    try:
        df = datastore.get(copy=False)
        return jsonify({
            "ok": True,
            "rows": int(len(df)),
            "cols": int(len(df.columns))
        }), 200
    except Exception as e:
        logger.exception("Healthcheck failed")
        return jsonify({"ok": False, "error": str(e)}), 500

# -----------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)

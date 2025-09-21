"""Dashboard blueprint and route handlers."""

from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from flask import (
    Blueprint,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from volta.utils.filter_params import FilterParams

bp = Blueprint("dashboard", __name__)


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
    selections: Dict[str, List[str]] = {}
    exclude_cols = current_app.config["EXCLUDE_COLS"]
    for column in base_df.columns:
        if column in exclude_cols:
            continue
        values = args.getlist(column)
        if values:
            selections[column] = [str(v) for v in values]

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
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()
    base = datastore.get(copy=True)

    if base.empty:
        return render_template("upload.html")

    if request.args and no_filters_selected(request.args, base):
        return redirect(url_for("dashboard.index"))

    params = build_params(request.args, base)
    after = params.apply(base, date_col)

    unique_values = build_unique_values(after)

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


@bp.route("/chart-data", methods=["GET"])
def chart_data():
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    metrics = get_metrics()
    base = datastore.get(copy=False)
    params = build_params(request.args, base)
    filtered = params.apply(base, date_col)

    metric = metrics.validate(filtered, params.metric)
    if not metric or date_col not in filtered.columns or filtered.empty:
        return jsonify(
            {
                "labels": [],
                "values": [],
                "metric_label": params.metric or "",
                "date_col": date_col,
            }
        )

    series = filtered.dropna(subset=[date_col]).copy()
    series[date_col] = pd.to_datetime(series[date_col], errors="coerce")
    series = series.dropna(subset=[date_col])
    if series.empty:
        return jsonify(
            {
                "labels": [],
                "values": [],
                "metric_label": metrics.label(metric),
                "date_col": date_col,
            }
        )

    rule = current_app.config["FREQ_RULE"].get(params.freq, "D")

    ts = series.set_index(series[date_col])
    grp = (
        ts[metric]
        .resample(rule, label="left", closed="left")
        .mean()
        .dropna()
        .sort_index()
    )

    if grp is None or len(grp) == 0:
        return jsonify(
            {
                "labels": [],
                "values": [],
                "metric_label": metrics.label(metric),
                "date_col": date_col,
            }
        )

    labels = [
        idx.strftime("%Y-%m") if params.freq == "M" else idx.date().isoformat()
        for idx in grp.index
    ]
    values = [float(v) for v in grp.values]
    return jsonify(
        {
            "labels": labels,
            "values": values,
            "metric_label": metrics.label(metric),
            "date_col": date_col,
        }
    )


@bp.route("/pie-data", methods=["GET"])
def pie_data():
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


__all__ = ["bp"]
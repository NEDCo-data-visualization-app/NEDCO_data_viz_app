"""Dashboard index view."""

from __future__ import annotations

import pandas as pd
from flask import current_app, redirect, render_template, request, url_for

from . import bp, get_datastore, get_metrics
from .helpers import DEFAULT_METERID_LIMIT, build_params, build_unique_values, no_filters_selected


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


bp.add_url_rule("/", view_func=index, methods=["GET"])
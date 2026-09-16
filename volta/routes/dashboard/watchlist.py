"""Watch list of meters to check, and inspection outcomes (private view only)."""

from __future__ import annotations

from datetime import datetime, timezone

from flask import abort, flash, redirect, render_template, request, url_for

from ...services.watchlist import (
    OUTCOMES, PAGE_SIZE, SIGNAL_LABELS, SORTS, add_inspection, districts_and_tariffs, query_watchlist,
    watchlist_summary,
)
from ..auth import effective_public_mode
from . import bp, get_datastore
from .downloads import csv_response


def _require_private() -> None:
    if effective_public_mode():
        abort(403)


def _filters():
    return {
        "utility": (request.args.get("utility") or "").strip() or None,
        "tariff_type": (request.args.get("tariff_type") or "").strip() or None,
        "signal": (request.args.get("signal") or "").strip() or None,
        "inspected": (request.args.get("inspected") or "").strip() or None,
    }


@bp.route("/watchlist", methods=["GET"])
def watchlist():
    _require_private()
    datastore = get_datastore()
    if not datastore.get_columns():
        return render_template("upload.html")
    filters = _filters()
    sort = request.args.get("sort") if request.args.get("sort") in SORTS else "score"
    try:
        page = max(int(request.args.get("page") or 1), 1)
    except ValueError:
        page = 1
    result = query_watchlist(datastore, filters, sort=sort, offset=(page - 1) * PAGE_SIZE, limit=PAGE_SIZE)
    districts, tariffs = districts_and_tariffs(datastore)
    pages = max(1, -(-result["total"] // PAGE_SIZE))
    return render_template(
        "watchlist.html", result=result, filters=filters, sort=sort, page=page, pages=pages,
        summary=watchlist_summary(datastore), districts=districts, tariffs=tariffs,
        signal_labels=SIGNAL_LABELS, outcomes=OUTCOMES,
    )


@bp.route("/watchlist.csv", methods=["GET"])
def watchlist_csv():
    _require_private()
    datastore = get_datastore()
    filters = _filters()
    sort = request.args.get("sort") if request.args.get("sort") in SORTS else "score"
    result = query_watchlist(datastore, filters, sort=sort, limit=None)
    cols = ["meterid", "customer_no", "utility", "tariff_type", "score", "signals", "shortfall_kwh", "last_purchase",
            "months_since", "kwh_12", "kwh_prev_12", "change_pct", "peer_kwh_12", "peer_pct", "purchases",
            "inspection_outcome", "inspection_at", "inspection_by"]

    def rows():
        out = []
        for r in result["rows"]:
            r = dict(r)
            r["signals"] = "; ".join(SIGNAL_LABELS[s] for s in r["signals"])
            for k in ("shortfall_kwh", "kwh_12", "kwh_prev_12", "peer_kwh_12", "change_pct", "peer_pct"):
                if r.get(k) is not None:
                    r[k] = round(float(r[k]), 1)
            out.append(tuple(r.get(c) for c in cols))
        return [(cols, out)]

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    return csv_response(rows(), f"watchlist_{stamp}.csv")


@bp.route("/customers/<meterid>/inspection", methods=["POST"])
def record_inspection(meterid: str):
    _require_private()
    datastore = get_datastore()
    try:
        add_inspection(datastore, meterid, request.form.get("outcome") or "", request.form.get("note") or "",
                       request.form.get("recorded_by") or "")
    except ValueError:
        flash("Choose an outcome before saving.", "warning")
    else:
        flash("Inspection outcome saved.", "success")
    return redirect(url_for("dashboard.customer", meterid=meterid))

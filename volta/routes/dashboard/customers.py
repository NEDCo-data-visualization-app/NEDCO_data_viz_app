"""Customer lookup and account pages (private view only)."""

from __future__ import annotations

from flask import abort, current_app, redirect, render_template, request, url_for

from ...services.customers import search_customers, customer_account
from ..auth import effective_public_mode
from . import bp, get_datastore


def _require_private() -> None:
    """Identifiers are shown on these pages, so the public view never gets them."""
    if effective_public_mode():
        abort(403)


@bp.route("/customers", methods=["GET"])
def customers():
    _require_private()
    datastore = get_datastore()
    if not datastore.get_columns():
        return render_template("upload.html")
    q = (request.args.get("q") or "").strip()
    result = search_customers(datastore, q)
    rows = result["rows"]
    if len(rows) == 1 and str(rows[0]["meterid"]) == q:
        return redirect(url_for("dashboard.customer", meterid=q))
    return render_template("customers.html", result=result, q=q)


@bp.route("/customers/<meterid>", methods=["GET"])
def customer(meterid: str):
    _require_private()
    datastore = get_datastore()
    if not datastore.get_columns():
        return render_template("upload.html")
    history = "all" if request.args.get("history") == "all" else "24m"
    account = customer_account(datastore, meterid, history=history)
    if account is None:
        abort(404)
    metrics = current_app.extensions["metrics"]
    return render_template("customer.html", a=account, metric_labels=metrics.mapping)

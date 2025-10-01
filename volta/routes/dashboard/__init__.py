"""Dashboard blueprint package."""

from __future__ import annotations

from flask import Blueprint

bp = Blueprint("dashboard", __name__)


def get_metrics():
    from flask import current_app

    return current_app.extensions["metrics"]


def get_datastore():
    from flask import current_app

    return current_app.extensions["datastore"]


from . import aggregates, charts, downloads, filters, health, meterid, views  # noqa: E402,F401

__all__ = ["bp", "get_metrics", "get_datastore"]
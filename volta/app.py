"""Application factory for the Volta dashboard."""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Mapping, Optional, Union

from flask import Flask, get_flashed_messages

from .config import Config
from .routes.auth import (
    auth_bp,
    effective_public_mode,
    has_private_access,
    is_authenticated,
    login_required_enabled,
    require_viewer_login,
)
from .routes.dashboard import bp as dashboard_bp
from .routes.dashboard import aggregates, charts, customers, downloads, export, filters, health, meterid, views, watchlist  # noqa: F401 - registers routes
from .routes.upload import upload_bp
from .services.datastore import DataStore
from .services.kpis import fmt_compact, fmt_full
from .services.metrics import Metrics

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("volta")


def create_app(config_object: Optional[Union[str, Mapping[str, Any], type]] = None) -> Flask:
    """Create and configure the Flask application."""
    if getattr(sys, "frozen", False):
        base = getattr(sys, "_MEIPASS")
        app = Flask(
            __name__,
            template_folder=os.path.join(base, "volta", "templates"),
            static_folder=os.path.join(base, "volta", "static"),
        )
    else:
        app = Flask(__name__)

    if config_object is None:
        app.config.from_object(Config)
    elif isinstance(config_object, Mapping):
        app.config.from_object(Config)
        app.config.from_mapping(config_object)
    else:
        app.config.from_object(config_object)

    metrics = Metrics(app.config["METRICS"])
    app.extensions["metrics"] = metrics
    app.extensions["datastore"] = DataStore(config=app.config, metrics=metrics)

    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(upload_bp)
    app.before_request(require_viewer_login)

    app.jinja_env.filters["compact"] = fmt_compact
    app.jinja_env.filters["full"] = fmt_full

    @app.context_processor
    def _inject_globals():
        gate = login_required_enabled()
        signed_in = is_authenticated()
        return {
            "is_public": effective_public_mode(),
            "show_forecasts": bool(app.config.get("SHOW_FORECASTS", False)),
            "admin_token_required": bool(app.config.get("ADMIN_TOKEN")),
            "show_logout": gate and signed_in,
            "login_gate_active": gate and not signed_in,
            "private_session": has_private_access(),
            "data_extent": app.extensions["datastore"].data_extent(),
            "flashes": get_flashed_messages(with_categories=True),
        }

    return app


__all__ = ["create_app"]

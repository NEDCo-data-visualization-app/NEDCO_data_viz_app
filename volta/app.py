"""Application factory for the Volta dashboard."""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Mapping, Optional, Union

from flask import Flask, get_flashed_messages

from .config import Config
from .routes.dashboard import bp as dashboard_bp
from .routes.dashboard import aggregates, charts, downloads, filters, health, meterid, views  # noqa: F401 - registers routes
from .routes.upload import upload_bp
from .services.datastore import DataStore
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

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(upload_bp)

    @app.context_processor
    def _inject_globals():
        return {"is_public": app.config.get("PUBLIC_MODE", False), "flashes": get_flashed_messages(with_categories=True)}

    return app


__all__ = ["create_app"]

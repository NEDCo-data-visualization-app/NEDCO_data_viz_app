"""Application factory for the Volta dashboard."""

from __future__ import annotations

import logging

from typing import Any, Mapping, Optional, Union

from flask import Flask

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("volta")

from .config import Config
from .routes.dashboard import bp as dashboard_bp
from .services.datastore import DataStore
from .services.metrics import Metrics


def create_app(
    config_object: Optional[Union[str, Mapping[str, Any], type]] = None,
) -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__)

    if config_object is None:
        app.config.from_object(Config)
    elif isinstance(config_object, Mapping):
        app.config.from_mapping(config_object)
    else:
        app.config.from_object(config_object)

    metrics = Metrics(app.config["METRICS"])
    datastore = DataStore(app.config, metrics)

    app.extensions["metrics"] = metrics
    app.extensions["datastore"] = datastore

    app.register_blueprint(dashboard_bp)

    return app


__all__ = ["create_app"]

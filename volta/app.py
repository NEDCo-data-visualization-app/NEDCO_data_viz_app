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
from .routes.dashboard import bp
from .services.datastore import DataStore
from .services.metrics import Metrics
import os
import sys
from .routes.upload import upload_bp


def create_app(
    config_object: Optional[Union[str, Mapping[str, Any], type]] = None,
) -> Flask:
    """Create and configure the Flask application."""

    import os, sys
    from .config import Config
    from .services.datastore import DataStore
    from .services.metrics import Metrics

    from .routes.dashboard import bp as dashboard_bp
    # Import submodules now to register routes
    from .routes.dashboard import aggregates, charts, downloads, filters, health, meterid, views
    from .routes.upload import upload_bp

    if getattr(sys, "frozen", False):
        template_folder = os.path.join(sys._MEIPASS, "volta", "templates")
        static_folder = os.path.join(sys._MEIPASS, "volta", "static")
        app = Flask(__name__, template_folder=template_folder, static_folder=static_folder)
    else:
        app = Flask(__name__)

    # -----------------------
    # Config
    # -----------------------
    if config_object is None:
        app.config.from_object(Config)
    elif isinstance(config_object, Mapping):
        app.config.from_mapping(config_object)
    else:
        app.config.from_object(config_object)

    # -----------------------
    # Initialize extensions
    # -----------------------
    metrics = Metrics(app.config["METRICS"])
    datastore = DataStore(config=app.config, metrics=metrics)

    app.extensions["metrics"] = metrics
    app.extensions["datastore"] = datastore

    # -----------------------
    # Register blueprints (after importing submodules)
    # -----------------------
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(upload_bp)

    return app



__all__ = ["create_app"]

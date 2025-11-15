"""Dashboard blueprint package."""

from __future__ import annotations
from pathlib import Path
from flask import Blueprint

bp = Blueprint("dashboard", __name__)


def get_metrics():
    from flask import current_app

    return current_app.extensions["metrics"]


def get_datastore():
    from flask import current_app

    return current_app.extensions["datastore"]

def get_predictor():
    """Return a cached predictor instance for running forecasts."""

    from flask import current_app

    predictor = current_app.extensions.get("predictor")
    if predictor is not None:
        return predictor

    from ...services.predictor import PredictorLGBM

    app_root = Path(current_app.root_path).resolve()
    project_root = app_root.parent

    def _resolve_path(value, fallback):
        path = Path(value) if value is not None else Path(fallback)
        if not path.is_absolute():
            path = project_root / path
        return path

    config = current_app.config
    db_path = _resolve_path(
        config.get("DUCKDB_PATH", "data/warehouse_new.duckdb"),
        "data/warehouse_new.duckdb",
    )

    bundle_path = config.get("PREDICTOR_BUNDLE_PATH")
    bundle_path = _resolve_path(bundle_path, "models/lgbm_bundle.pkl")

    features_path = config.get("PREDICTOR_FEATURES_PATH")
    features_path = _resolve_path(features_path, "models/feature_cols.json")

    predictor = PredictorLGBM(
        db_path=db_path,
        bundle_path=bundle_path,
        features_path=features_path,
    )

    current_app.extensions["predictor"] = predictor
    return predictor


from . import aggregates, charts, downloads, filters, health, meterid, views  # noqa: E402,F401

__all__ = ["bp", "get_metrics", "get_datastore", "get_predictor"]
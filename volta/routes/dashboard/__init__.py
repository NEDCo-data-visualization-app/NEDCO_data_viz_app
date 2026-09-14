"""Dashboard blueprint package."""

from __future__ import annotations

from threading import Lock
from typing import Optional

from flask import Blueprint, current_app

bp = Blueprint("dashboard", __name__)


def get_metrics():
    return current_app.extensions["metrics"]


def get_datastore():
    return current_app.extensions["datastore"]


_predictor_instance: Optional["PredictorLGBM"] = None  # noqa: F821
_predictor_lock = Lock()


def get_predictor():
    """Lazily build the LightGBM predictor (loading 36 pickles is slow, so do it once)."""
    global _predictor_instance
    if _predictor_instance is not None:
        return _predictor_instance

    with _predictor_lock:
        if _predictor_instance is None:
            from ...services.predictor import PredictorLGBM

            config = current_app.config
            _predictor_instance = PredictorLGBM(
                model_dir=config["MODEL_DIR"],
                db_path=config["DB_PATH"],
                raw_table=config["PARQUET_PATH"],
            )
    return _predictor_instance


__all__ = ["bp", "get_metrics", "get_datastore", "get_predictor"]

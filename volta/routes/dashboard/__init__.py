"""Dashboard blueprint package."""

from __future__ import annotations
from pathlib import Path
from flask import Blueprint, current_app
import traceback
import inspect
from typing import Optional

bp = Blueprint("dashboard", __name__)

# -------------------------------
# Metrics / datastore accessors
# -------------------------------
def get_metrics():
    return current_app.extensions["metrics"]

def get_datastore():
    return current_app.extensions["datastore"]

# -------------------------------
# Predictor singleton (lazy)
# -------------------------------

from threading import Lock
from typing import Optional
import inspect
from flask import current_app
_predictor_instance: Optional["PredictorLGBM"] = None
_predictor_lock = Lock() 

def get_predictor():
    global _predictor_instance

    # If already initialized, return it
    if _predictor_instance is not None:
        return _predictor_instance

    # Thread-safe singleton creation
    with _predictor_lock:
        # Check again inside lock in case another thread already set it
        if _predictor_instance is not None:
            return _predictor_instance

        # Optional: log stack for debugging
        print("get_predictor() creating predictor! Call stack:")
        for frame_info in inspect.stack():
            print(f"  File {frame_info.filename}, line {frame_info.lineno}, in {frame_info.function}")

        # Lazy import
        from ...services.predictor import PredictorLGBM
        config = current_app.config

        _predictor_instance = PredictorLGBM(
            model_dir=config.get("PREDICTOR_MODEL_DIR", "models"),
            db_path=config.get("DUCKDB_PATH", "data/warehouse_new.duckdb"),
            raw_table=config.get("PREDICTOR_RAW_TABLE", "merged_sales_customers_clean"),
        )

    return _predictor_instance


__all__ = ["bp", "get_metrics", "get_datastore", "get_predictor", "register_blueprint_routes"]

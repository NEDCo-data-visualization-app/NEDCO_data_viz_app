"""Application configuration.

Environment variables (a ``.env`` file next to the executable or in the project
root is honoured):

    DB_PATH        DuckDB file, default ``data/warehouse_new.duckdb``
    PARQUET_PATH   Name of the DuckDB table holding the dataset
                   (legacy variable name), default ``merged_sales_customers_clean``
    PUBLIC_MODE    ``true`` hides meter identifiers from the UI
    BUCKET_URL     Optional remote parquet export used by "Try Internet Connection"
    SUPABASE_KEY   Optional API key sent with the BUCKET_URL request
    ADMIN_TOKEN    When set, CSV uploads and remote refreshes require this
                   token (use it for any deployment reachable by others)
    VIEWER_PASSWORD  When set, every page requires signing in with this
                   shared password (sessions last 30 days)
    PRIVATE_PASSWORD Optional second password that also unlocks the private
                   view (meter and customer identifiers) for that session
    SESSION_COOKIE_SECURE  Set to true behind HTTPS (e.g. on Render)
    UPLOADS_DIR    Where uploaded CSVs are staged before ingestion
    MODEL_DIR      Folder containing the LightGBM pickles, default ``models``
    FORECAST_AS_OF Cut-off date separating history from forecast on the
                   predictions page, default ``2020-09-01``
    VOLTA_DATE_COL / VOLTA_DATE_FMT  Date column name and the strptime format
                   used when a CSV upload carries non-ISO dates
"""

from __future__ import annotations

import os
import secrets
import shutil
import sys
from datetime import timedelta
from pathlib import Path
from typing import Dict, Optional

from dotenv import load_dotenv


def _bundle_dir() -> Optional[Path]:
    """Extraction folder of a PyInstaller one-file build, if we are frozen."""
    if getattr(sys, "frozen", False):
        return Path(getattr(sys, "_MEIPASS"))
    return None


def _app_dir() -> Path:
    """Folder the app is launched from: beside the executable when frozen, else the repo root."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


# A .env beside the executable (or in the repo root) wins over the one baked
# into the bundle, because load_dotenv never overrides variables already set.
load_dotenv(_app_dir() / ".env")
_bundle = _bundle_dir()
if _bundle is not None:
    load_dotenv(_bundle / ".env")


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def resolve_data_path(value: str, copy_from_bundle: bool = False) -> Path:
    """Resolve a data path so it works both from source and from a frozen build.

    Absolute paths are used as-is. Relative paths are resolved beside the
    executable (or the repo root). When frozen and the file only exists inside
    the bundle, it is copied next to the executable once so that later writes
    (uploads) persist between launches; if that copy fails the bundled copy is
    used directly.
    """
    path = Path(value).expanduser()
    if path.is_absolute():
        return path

    local = _app_dir() / path
    bundle = _bundle_dir()
    if local.exists() or bundle is None:
        return local

    bundled = bundle / path
    if not bundled.exists():
        return local

    if copy_from_bundle and bundled.is_file():
        try:
            local.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(bundled, local)
            return local
        except OSError:
            pass
    return bundled


class Config:
    """Base configuration for the Volta dashboard."""

    SECRET_KEY = os.getenv("SECRET_KEY") or secrets.token_hex(16)
    PUBLIC_MODE = _env_flag("PUBLIC_MODE", False)
    ADMIN_TOKEN = os.getenv("ADMIN_TOKEN") or None
    VIEWER_PASSWORD = os.getenv("VIEWER_PASSWORD") or None
    PRIVATE_PASSWORD = os.getenv("PRIVATE_PASSWORD") or None
    SESSION_COOKIE_SECURE = _env_flag("SESSION_COOKIE_SECURE", False)
    SESSION_COOKIE_SAMESITE = "Lax"
    PERMANENT_SESSION_LIFETIME = timedelta(days=30)

    # -------------------------
    # Data
    # -------------------------
    DB_PATH = str(resolve_data_path(os.getenv("DB_PATH") or "data/warehouse_new.duckdb", copy_from_bundle=True))
    PARQUET_PATH = os.getenv("PARQUET_PATH") or "merged_sales_customers_clean"
    UPLOADS_DIR = Path(os.getenv("UPLOADS_DIR") or (Path.home() / "Downloads" / "volta" / "uploads"))

    DATE_COL = os.getenv("VOLTA_DATE_COL", "od_date")
    DATE_FMT = os.getenv("VOLTA_DATE_FMT", "%d-%b-%y")

    # Optional remote refresh
    BUCKET_URL = os.getenv("BUCKET_URL") or None
    SUPABASE_KEY = os.getenv("SUPABASE_KEY") or None

    # Forecasting
    MODEL_DIR = str(resolve_data_path(os.getenv("MODEL_DIR") or "models"))
    FORECAST_AS_OF = os.getenv("FORECAST_AS_OF", "2020-09-01")

    # -------------------------
    # UI
    # -------------------------
    METERID_MAX_OPTIONS = 500

    # Hidden from the checkbox filters
    EXCLUDE_COLS = {
        "od_date",
        "od_date_str",
        "month",
        "month_str",
        "year",
        "ocd_energy",
        "ocd_cash_received",
        "ocd_paymoney",
    }

    METRICS: Dict[str, str] = {
        "ocd_energy": "Energy (kWh)",
        "ocd_paymoney": "Paymoney",
        "ocd_cash_received": "Cash Received (GHC)",
    }


__all__ = ["Config", "resolve_data_path"]

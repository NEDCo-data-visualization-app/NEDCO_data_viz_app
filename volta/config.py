"""Application configuration objects."""

import os
import sys
from typing import Dict
from dotenv import load_dotenv
from pathlib import Path

if getattr(sys, "frozen", False):
    load_dotenv(os.path.join(sys._MEIPASS, ".env"))
else:
    load_dotenv()


class Config:
    """Base configuration for the Volta dashboard."""

    # -------------------------
    # Data paths
    # -------------------------
    # DuckDB database file
    DUCKDB_PATH = Path(os.getenv("VOLTA_DUCKDB_PATH", "data/warehouse.duckdb"))

    # Location of incoming CSVs (from client uploads)
    CSV_GLOB = os.getenv("VOLTA_CSV_GLOB", "data/*.csv")

    # (Legacy) still allow Parquet path for backwards compatibility
    DATA_PATH = os.getenv("VOLTA_DATA_PATH", "data/wkfile_shiny.parquet")

    # -------------------------
    # Data schema
    # -------------------------
    DATE_COL = os.getenv("VOLTA_DATE_COL", "chargedate")

    # -------------------------
    # External services
    # -------------------------
    BUCKET_URL = os.getenv("BUCKET_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    # -------------------------
    # UI filters
    # -------------------------
    # Hide these from the checkbox UI
    EXCLUDE_COLS = {
        "chargedate",
        "chargedate_str",
        "month",
        "month_str",
        "year",
        "kwh",
        "ghc",
        "paymoney",
        "res",
    }

    RES_MAP = {"N-Resid [0]": "Commercial", "Resid [1]": "Residential"}

    # -------------------------
    # Centralized metrics & frequency config
    # -------------------------
    METRICS: Dict[str, str] = {
        "kwh": "kWh",
        "paymoney": "Pay",
        "ghc": "GHC",
    }

    FREQ_RULE: Dict[str, str] = {
        "D": "D",
        "W": "W-MON",  # weekly anchored to Monday
        "M": "M",
    }


__all__ = ["Config"]

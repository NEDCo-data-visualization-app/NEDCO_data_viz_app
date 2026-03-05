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
    # DuckDB database file (main store)

    # Location of incoming CSVs (from client uploads)
    UPLOADS_DIR = Path.home() / "Downloads" / "volta" / "uploads"
    CSV_GLOB = str(UPLOADS_DIR / "*.csv")

    # -------------------------
    # Data schema
    # -------------------------
    DATE_COL = os.getenv("VOLTA_DATE_COL", "od_date")

    # -------------------------
    # External services (optional legacy path)
    # -------------------------
    BUCKET_URL = os.getenv("BUCKET_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    PARQUET_PATH = os.getenv("PARQUET_PATH")
    DB_PATH = os.getenv("DB_PATH")

    # -------------------------
    # UI filters
    # -------------------------
    # Hide these from the checkbox UI
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

    # -------------------------
    # Centralized metrics & frequency config
    # -------------------------
    METRICS: Dict[str, str] = {
        "ocd_energy": "Energy (kWh)",
        "ocd_paymoney": "Paymoney",
        "ocd_cash_received": "Cash Received (GHC)",
    }

    FREQ_RULE: Dict[str, str] = {
        "D": "D",
        "W": "W-MON",  # weekly anchored to Monday
        "M": "M",
    }
    

__all__ = ["Config"]

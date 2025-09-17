"""Application configuration objects."""

import os
from typing import Dict


class Config:
    """Base configuration for the Volta dashboard."""

    # You can override these with environment variables
    DATA_PATH = os.getenv("VOLTA_DATA_PATH", "data/wkfile_shiny.parquet")
    DATE_COL = os.getenv("VOLTA_DATE_COL", "chargedate")

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

    # Centralized metrics & frequency config
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
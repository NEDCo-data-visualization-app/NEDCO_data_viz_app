"""Data access and aggregation helpers."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Mapping, Optional, Union
import requests
from io import BytesIO
import pandas as pd

#from volta.routes.metrics import Metrics

logger = logging.getLogger("volta")


class DataStore:
    """Own data loading, preprocessing, derived stats, and in-memory caching."""

    def __init__(self, config: Mapping[str, Any], metrics: Metrics):
        self.config = config
        self.metrics = metrics
        self._df: Optional[pd.DataFrame] = None

    @staticmethod
    def _clean_nan_rows(df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(how="any").reset_index(drop=True)

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop_duplicates().reset_index(drop=True)
        df = self._clean_nan_rows(df)

        res_map = self.config.get("RES_MAP", {})
        if "res" in df.columns and res_map:
            df["res_mapped"] = df["res"].astype(str).map(res_map).fillna("Unknown")

        date_col = self.config.get("DATE_COL")
        if date_col and date_col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce", format="%d-%b-%y")

        for numcol in self.metrics.mapping.keys():
            if numcol in df.columns:
                df[numcol] = pd.to_numeric(df[numcol], errors="coerce")

        return df

    def load(self) -> pd.DataFrame:
        if self._df is not None:
            return self._df

        url = self.config.get("BUCKET_URL")       
        headers = {"apikey": self.config.get("SUPABASE_KEY")}  
        path = self.config.get("DATA_PATH")

        if url:
            try:
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()
                raw = pd.read_parquet(BytesIO(resp.content))
            except requests.HTTPError as e:
                if path and os.path.exists(path):
                    raw = pd.read_parquet(path)
                else:
                    raise FileNotFoundError("Cannot fetch remote or local file")
        elif path and os.path.exists(path):
            raw = pd.read_parquet(path)
        else:
            raise FileNotFoundError("No valid DATA_PATH or BUCKET_URL found")

        logger.info("Loaded raw DataFrame")
        self._df = self._preprocess(raw)
        logger.info("Processed DataFrame")
        return self._df

    def get(self, copy: bool = True) -> pd.DataFrame:
        df = self.load()
        return df.copy(deep=False) if copy else df

    def reload(self) -> None:
        self._df = None
        logger.info("DataStore cache cleared")

    def compute_stats(self, df: pd.DataFrame) -> Dict[str, Dict[str, Union[float, str]]]:
        stats: Dict[str, Dict[str, Union[float, str]]] = {}
        for key, label in self.metrics.mapping.items():
            if key in df.columns:
                s = pd.to_numeric(df[key], errors="coerce").dropna()
                if len(s) > 0:
                    stats[key] = {
                        "label": label,
                        "sum": float(s.sum()),
                        "mean": float(s.mean()),
                        "median": float(s.median()),
                        "min": float(s.min()),
                        "max": float(s.max()),
                    }
        return stats

    def compute_summary(self, df: pd.DataFrame) -> Dict[str, Union[int, str, None]]:
        date_col = self.config.get("DATE_COL")
        out: Dict[str, Union[int, str, None]] = {
            "rows": len(df),
            "cols": len(df.columns),
            "meters": (df["meterid"].nunique() if "meterid" in df.columns else None),
            "locations": (df["loc"].nunique() if "loc" in df.columns else None),
            "date_min": "",
            "date_max": "",
        }
        if date_col and date_col in df.columns and len(df) > 0:
            dmin = pd.to_datetime(df[date_col], errors="coerce").min()
            dmax = pd.to_datetime(df[date_col], errors="coerce").max()
            if pd.notna(dmin):
                out["date_min"] = dmin.date().isoformat()
            if pd.notna(dmax):
                out["date_max"] = dmax.date().isoformat()
        return out


__all__ = ["DataStore"]
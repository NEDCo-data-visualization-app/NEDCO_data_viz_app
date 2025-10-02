"""Metrics service utilities."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pandas as pd


class Metrics:
    """Encapsulate metric mapping and helper routines."""

    def __init__(self, mapping: Dict[str, str]):
        self.mapping = dict(mapping)

    def label(self, key: Optional[str]) -> str:
        if not key:
            return ""
        return self.mapping.get(key, key)

    def validate(self, df, metric: Optional[str]) -> Optional[str]:
        """Check if a metric is valid for this dataset (pandas or duckdb relation)."""
        if not metric:
            return None

        # Works for pandas or duckdb relation
        columns = getattr(df, "columns", None)
        if columns is None and hasattr(df, "columns"):
            columns = df.columns

        if columns is not None and metric in columns and metric in self.mapping:
            return metric
        return None

    def available(self, df: pd.DataFrame) -> List[Tuple[str, str]]:
        return [(k, v) for k, v in self.mapping.items() if k in df.columns]


__all__ = ["Metrics"]
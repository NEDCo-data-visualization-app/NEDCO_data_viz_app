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

    def validate(self, df: pd.DataFrame, metric: Optional[str]) -> Optional[str]:
        if not metric:
            return None
        return metric if (metric in df.columns and metric in self.mapping) else None

    def available(self, df: pd.DataFrame) -> List[Tuple[str, str]]:
        return [(k, v) for k, v in self.mapping.items() if k in df.columns]


__all__ = ["Metrics"]
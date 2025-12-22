from __future__ import annotations
from typing import Dict, List, Optional, Tuple, Sequence, Mapping, Any

class Metrics:
    """Encapsulate metric mapping and helper routines for list-of-dicts datasets."""

    def __init__(self, mapping: Dict[str, str]):
        """
        mapping: dict of metric_key -> human-readable label
        Example: {"amount": "Amount", "price": "Price"}
        """
        self.mapping = dict(mapping)

    def label(self, key: Optional[str]) -> str:
        """Return the human-readable label for a metric key, or the key itself."""
        if not key:
            return ""
        return self.mapping.get(key, key)

    def validate(self, rows: Sequence[Mapping[str, Any]], metric: Optional[str]) -> Optional[str]:
        """
        Check if a metric is valid for this dataset.
        - rows: list of dicts from DataStore.run_query()
        - metric: the key to validate
        Returns the metric if valid, otherwise None
        """
        if not metric or not rows:
            return None

        first_row = rows[0]
        # Must exist in both first row and mapping
        if metric in first_row and metric in self.mapping:
            return metric
        return None

    def available(self, rows: Sequence[Mapping[str, Any]]) -> List[Tuple[str, str]]:
        """
        List all available metrics present in the dataset.
        Returns list of (metric_key, human_label)
        """
        if not rows:
            return []
        first_row = rows[0]
        return [(k, v) for k, v in self.mapping.items() if k in first_row]

    def keys(self) -> List[str]:
        """Return just the available metric keys."""
        return list(self.mapping.keys()) 

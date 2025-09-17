# filter_params.py
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional

# If you used Literal in the earlier version, keep it optional-friendly:
try:
    from typing import Literal
except ImportError:  # Python <3.8 w/o backport
    from typing_extensions import Literal  # pip install typing_extensions if needed

import pandas as pd


Freq = Literal["D", "W", "M"]  # daily, weekly, monthly


@dataclass(frozen=True)
class FilterParams:
    start: Optional[date] = None
    end: Optional[date] = None
    selections: Dict[str, List[str]] = field(default_factory=dict)
    freq: Freq = "D"
    metric: Optional[str] = None

    def apply(self, df: pd.DataFrame, date_col: str) -> pd.DataFrame:
        """Return a filtered dataframe based on the stored parameters."""
        out = df

        # Categorical selections
        for col, vals in self.selections.items():
            if col in out.columns and vals:
                out = out[out[col].astype(str).isin(vals)]

        # Date range
        if date_col in out.columns:
            if self.start is not None:
                out = out[out[date_col] >= pd.Timestamp(self.start)]
            if self.end is not None:
                out = out[out[date_col] <= pd.Timestamp(self.end)]

        return out

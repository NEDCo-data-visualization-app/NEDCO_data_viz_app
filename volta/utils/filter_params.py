# filter_params.py
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Tuple, Iterable

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

    # -------- pandas path (unchanged) --------
    def apply(self, df: pd.DataFrame, date_col: str) -> pd.DataFrame:
        """Return a filtered dataframe based on the stored parameters (pandas)."""
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

    # -------- SQL helpers (new) --------
    def trunc_unit(self) -> str:
        """Map UI frequency to DuckDB date_trunc unit."""
        return {"D": "day", "W": "week", "M": "month"}.get(self.freq, "day")

    def to_sql_where(
        self,
        date_col: str,
        available_columns: Optional[Iterable[str]] = None,
    ) -> Tuple[str, List[str]]:
        """
        Build a safe SQL WHERE clause and its parameters for DuckDB.

        - Includes date range if provided.
        - Includes equality filters for any selections whose columns exist in `available_columns`.
        - Returns ("<where expr>", [params...]).
        """
        where = []
        params: List[str] = []

        # Date range (inclusive)
        if self.start is not None and self.end is not None:
            where.append(f"{date_col} BETWEEN ? AND ?")
            params.extend([pd.Timestamp(self.start).date().isoformat(),
                           pd.Timestamp(self.end).date().isoformat()])
        elif self.start is not None:
            where.append(f"{date_col} >= ?")
            params.append(pd.Timestamp(self.start).date().isoformat())
        elif self.end is not None:
            where.append(f"{date_col} <= ?")
            params.append(pd.Timestamp(self.end).date().isoformat())

        cols = set(available_columns) if available_columns is not None else None

        # Categorical selections (col IN (?,?,...))
        for col, vals in (self.selections or {}).items():
            if not vals:
                continue
            if cols is not None and col not in cols:
                continue
            placeholders = ",".join(["?"] * len(vals))
            where.append(f"{col} IN ({placeholders})")
            params.extend([str(v) for v in vals])

        clause = " AND ".join(where) if where else "1=1"
        return clause, params

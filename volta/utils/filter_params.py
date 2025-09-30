# filter_params.py
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Tuple, Iterable

try:
    from typing import Literal
except ImportError:  # Python <3.8 w/o backport
    from typing_extensions import Literal

import pandas as pd

Freq = Literal["D", "W", "M"]  # daily, weekly, monthly


@dataclass(frozen=True)
class FilterParams:
    start: Optional[date] = None
    end: Optional[date] = None
    selections: Dict[str, List[str]] = field(default_factory=dict)
    freq: Freq = "D"
    metric: Optional[str] = None

    # -------- pandas path --------
    def apply(self, df: pd.DataFrame, date_col: str) -> pd.DataFrame:
        """
        Return a filtered dataframe based on the stored parameters (pandas).

        Applies INTERSECTION (AND) across:
          - All categorical selections (meterid, loc, res_mapped, etc.)
          - Date range [start, end] inclusive

        Handles dtype mismatches by comparing categorical columns as strings.
        """
        out = df

        # Normalize all selection values to strings
        normalized: Dict[str, List[str]] = {
            col: [str(v) for v in vals if v not in (None, "")]
            for col, vals in (self.selections or {}).items()
            if vals
        }

        # Apply categorical filters
        for col, vals in normalized.items():
            if col in out.columns and vals:
                out = out[out[col].astype(str).isin(vals)]

        # Apply date range (inclusive)
        if date_col in out.columns:
            if self.start is not None:
                out = out[out[date_col] >= pd.Timestamp(self.start)]
            if self.end is not None:
                out = out[out[date_col] <= pd.Timestamp(self.end)]

        return out

    # -------- SQL helpers --------
    def trunc_unit(self) -> str:
        """Map UI frequency to DuckDB date_trunc unit."""
        return {"D": "day", "W": "week", "M": "month"}.get(self.freq, "day")

    def to_sql_where(
        self,
        date_col: str,
        available_columns: Optional[Iterable[str]] = None,
    ) -> Tuple[str, List[str]]:
        """
        Build a safe SQL WHERE clause and its parameters (DuckDB compatible).

        INTERSECTION (AND) of:
          - date range (inclusive) on date_col
          - categorical selections as IN-lists

        Robust to dtype mismatches by casting selected columns to VARCHAR.
        """
        where: List[str] = []
        params: List[str] = []

        # Date range (inclusive)
        if self.start is not None and self.end is not None:
            where.append(f"CAST({date_col} AS DATE) BETWEEN ? AND ?")
            params.extend([
                pd.Timestamp(self.start).date().isoformat(),
                pd.Timestamp(self.end).date().isoformat(),
            ])
        elif self.start is not None:
            where.append(f"CAST({date_col} AS DATE) >= ?")
            params.append(pd.Timestamp(self.start).date().isoformat())
        elif self.end is not None:
            where.append(f"CAST({date_col} AS DATE) <= ?")
            params.append(pd.Timestamp(self.end).date().isoformat())

        cols = set(available_columns) if available_columns is not None else None

        # Normalize all selection values to strings
        normalized: Dict[str, List[str]] = {
            col: [str(v) for v in vals if v not in (None, "")]
            for col, vals in (self.selections or {}).items()
            if vals
        }

        # Apply categorical selections (safe with CAST)
        for col, vals in normalized.items():
            if not vals:
                continue
            if cols is not None and col not in cols:
                continue
            placeholders = ",".join(["?"] * len(vals))
            where.append(f"CAST({col} AS VARCHAR) IN ({placeholders})")
            params.extend(vals)

        clause = " AND ".join(where) if where else "1=1"
        return clause, params

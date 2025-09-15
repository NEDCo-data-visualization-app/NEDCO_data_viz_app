from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Literal

Freq = Literal["D", "W", "M"]  # daily, weekly, monthly

@dataclass(frozen=True)
class FilterParams:
    # date range
    start: Optional[date] = None
    end: Optional[date] = None
    # selected categorical values per column
    selections: Dict[str, List[str]] = field(default_factory=dict)
    # chart options (optional, used by /chart-data, /pie-data, /bar-data)
    freq: Freq = "D"
    metric: Optional[str] = None

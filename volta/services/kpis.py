"""Key figures for the manager overview.

Everything is computed in DuckDB over the current filter selection. The
dataset holds prepaid purchases, so "amount paid" is the revenue measure and
cash received is deliberately not surfaced here (it becomes relevant once
postpaid billing customers are included).
"""

from __future__ import annotations

import calendar
from dataclasses import replace
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from volta.utils.filter_params import FilterParams

# Order in which the tiles are shown; each maps to a key of ``current``.
TILE_KEYS = ["energy", "paymoney", "customers", "spend_per_customer_month", "price_per_kwh", "residential_share"]


def fmt_compact(value: Optional[float], decimals: int = 1) -> str:
    """1234567 -> '1.2 M'; small values keep thousands separators."""
    if value is None:
        return "—"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "—"
    sign = "-" if v < 0 else ""
    v = abs(v)
    for limit, suffix in ((1e9, " bn"), (1e6, " M"), (1e3, " k")):
        if v >= limit:
            return f"{sign}{v / limit:,.{decimals}f}{suffix}"
    if v == int(v):
        return f"{sign}{int(v):,}"
    return f"{sign}{v:,.{decimals}f}"


def fmt_full(value: Optional[float], decimals: int = 0) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return "—"


def pct_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous in (None, 0):
        return None
    return (float(current) - float(previous)) / abs(float(previous)) * 100.0


def _month_start(d: date) -> date:
    return d.replace(day=1)


def _shift_months(d: date, months: int) -> date:
    """First day of the month ``months`` after (or before, if negative) d's month."""
    y, m = divmod(d.year * 12 + d.month - 1 + months, 12)
    return date(y, m + 1, 1)


def _month_end(d: date) -> date:
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def months_between(start: date, end: date) -> int:
    """Calendar months touched by [start, end], at least 1."""
    return max(1, (end.year - start.year) * 12 + end.month - start.month + 1)


def period_presets(date_min: Optional[date], date_max: Optional[date]) -> List[Dict[str, str]]:
    """Quick date ranges relative to the newest data, for the filter bar."""
    if not date_min or not date_max:
        return []
    presets = [
        ("Last month", _month_start(date_max), date_max),
        ("Last 3 months", _shift_months(date_max, -2), date_max),
        ("Last 12 months", _shift_months(date_max, -11), date_max),
        ("Last 24 months", _shift_months(date_max, -23), date_max),
        ("All data", date_min, date_max),
    ]
    out = []
    for label, start, end in presets:
        start = max(start, date_min)
        out.append({"label": label, "start": start.isoformat(), "end": end.isoformat()})
    return out


def _previous_window(start: date, end: date) -> tuple[date, date]:
    """The comparable window immediately before ``start``.

    Ranges starting on the 1st shift back by whole months so that the
    comparison lines up with calendar months: 1 Jan to 31 Dec compares with
    the previous calendar year, and 1 Oct 2019 to 12 Sep 2020 ("last 12
    months" to date) compares with 1 Oct 2018 to 12 Sep 2019. Any other range
    shifts back by its own length in days.
    """
    if start.day == 1:
        n = months_between(start, end)
        prev_start = _shift_months(start, -n)
        if end == _month_end(end):
            return prev_start, start - timedelta(days=1)
        shifted = _shift_months(end, -n)  # same month one period earlier, same day of month
        prev_end = shifted.replace(day=min(end.day, calendar.monthrange(shifted.year, shifted.month)[1]))
        return prev_start, min(prev_end, start - timedelta(days=1))
    length = (end - start).days + 1
    prev_end = start - timedelta(days=1)
    return prev_end - timedelta(days=length - 1), prev_end


def _aggregate(datastore, columns: List[str], date_col: str, clause: str, sql_params) -> Dict[str, Any]:
    cols = set(columns)
    parts = ["COUNT(*) AS purchases"]
    parts.append("SUM(ocd_energy) AS energy" if "ocd_energy" in cols else "NULL AS energy")
    parts.append("SUM(ocd_paymoney) AS paymoney" if "ocd_paymoney" in cols else "NULL AS paymoney")
    if "meterid" in cols:
        parts.append("COUNT(DISTINCT meterid) AS customers")
        parts.append(f"COUNT(DISTINCT ROW(meterid, DATE_TRUNC('month', CAST({date_col} AS DATE)))) AS customer_months")
    else:
        parts.append("NULL AS customers")
        parts.append("NULL AS customer_months")
    parts.append("COUNT(DISTINCT utility) AS districts" if "utility" in cols else "NULL AS districts")
    parts.append(f"MIN(CAST({date_col} AS DATE)) AS date_min")
    parts.append(f"MAX(CAST({date_col} AS DATE)) AS date_max")

    sql = f"SELECT {', '.join(parts)} FROM {datastore.table_sql}"
    if clause:
        sql += f" WHERE {clause}"
    row = datastore.fetch_one(sql, sql_params) or {}

    def num(key):
        v = row.get(key)
        return None if v is None else float(v)

    out: Dict[str, Any] = {
        "purchases": int(row.get("purchases") or 0),
        "energy": num("energy"),
        "paymoney": num("paymoney"),
        "customers": None if row.get("customers") is None else int(row["customers"]),
        "customer_months": None if row.get("customer_months") is None else int(row["customer_months"]),
        "districts": None if row.get("districts") is None else int(row["districts"]),
        "date_min": row.get("date_min"),
        "date_max": row.get("date_max"),
    }
    cm = out["customer_months"] or 0
    out["spend_per_customer_month"] = (out["paymoney"] / cm) if out["paymoney"] is not None and cm else None
    out["price_per_kwh"] = (
        out["paymoney"] / out["energy"] if out["paymoney"] is not None and out["energy"] else None
    )

    # Customer mix by account type (share of active customers).
    out["mix"] = []
    out["residential_share"] = None
    if "tariff_type" in cols and "meterid" in cols and out["customers"]:
        mix_sql = (
            f"SELECT CAST(tariff_type AS VARCHAR) AS tariff_type, COUNT(DISTINCT meterid) AS customers "
            f"FROM {datastore.table_sql}"
        )
        if clause:
            mix_sql += f" WHERE {clause}"
        mix_sql += " GROUP BY 1 ORDER BY 2 DESC, 1"
        total = out["customers"]
        for r in datastore.run_query(mix_sql, sql_params):
            n = int(r["customers"] or 0)
            out["mix"].append({"tariff_type": r["tariff_type"] or "—", "customers": n, "share": n / total * 100.0})
        residential = [m for m in out["mix"] if str(m["tariff_type"]).strip().lower().startswith("res")]
        if residential:
            out["residential_share"] = sum(m["share"] for m in residential)
    return out


def compute_kpis(datastore, params: FilterParams, date_col: str, columns: List[str]) -> Dict[str, Any]:
    """Key figures for the current filters, plus the same figures for the preceding period."""
    clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=columns)
    current = _aggregate(datastore, columns, date_col, clause, sql_params)

    start = params.start or current.get("date_min")
    end = params.end or current.get("date_max")
    period: Dict[str, Any] = {"start": start, "end": end, "months": months_between(start, end) if start and end else 0}

    previous: Optional[Dict[str, Any]] = None
    if start and end and current["purchases"]:
        prev_start, prev_end = _previous_window(start, end)
        extent = datastore.data_extent()
        earliest = extent.get("date_min")
        if earliest is None or prev_end >= earliest:
            prev_params = replace(params, start=prev_start, end=prev_end)
            prev_clause, prev_sql_params = prev_params.to_sql_where(date_col=date_col, available_columns=columns)
            prev = _aggregate(datastore, columns, date_col, prev_clause, prev_sql_params)
            if prev["purchases"]:
                prev["start"], prev["end"] = prev_start, prev_end
                prev["months"] = months_between(prev_start, prev_end)
                prev["partial"] = bool(earliest and prev_start < earliest)
                previous = prev

    deltas: Dict[str, Optional[float]] = {}
    for key in TILE_KEYS:
        deltas[key] = pct_change(current.get(key), previous.get(key)) if previous else None

    return {"current": current, "previous": previous, "period": period, "deltas": deltas}


__all__ = ["compute_kpis", "period_presets", "fmt_compact", "fmt_full", "pct_change", "months_between"]

"""Customer lookup and account view for billing officers.

Everything here runs in DuckDB over the dataset table. Identifiers are
involved throughout, so the routes that use this module are private-view
only.
"""

from __future__ import annotations

import calendar
from datetime import date
from typing import Any, Dict, List, Optional

from volta.utils.filter_params import FilterParams

from .kpis import _shift_months, compute_kpis, months_between, pct_change

# Columns that are part of every transaction; anything else in the table is
# treated as a customer attribute and shown on the account page.
CORE_COLUMNS = {
    "meterid", "customer_no", "od_date", "ocd_energy", "ocd_paymoney", "ocd_cash_received", "utility", "tariff_type",
}
HIDDEN_COLUMNS = {
    "od_date_str", "chargedate_str", "month", "month_str", "year", "meterid_norm", "customer_meter_no_norm",
    "meter_no_norm",
}

SEARCH_LIMIT = 50
PURCHASES_LIMIT = 50
ACTIVE_MONTHS = 3      # a purchase within this many months of the data end = active
INACTIVE_MONTHS = 12   # no purchase for this long = inactive
DROP_THRESHOLD = 0.5   # consumption at less than half of a year earlier
PEER_LOW_THRESHOLD = 0.3  # buying under 30% of what similar customers buy


def _meter_clause(column: str = "meterid") -> str:
    return f"CAST({column} AS VARCHAR) = ?"


def detail_columns(columns: List[str]) -> List[str]:
    """Customer attribute columns present in the table (beyond the transaction fields)."""
    return [c for c in columns if c.lower() not in CORE_COLUMNS and c.lower() not in HIDDEN_COLUMNS]


def pretty_label(column: str) -> str:
    return column.replace("_", " ").strip().capitalize()


# ---------------------------------------------------------------- search
def search_customers(datastore, query: str, limit: int = SEARCH_LIMIT) -> Dict[str, Any]:
    """Meters whose meter number or customer number contains ``query``."""
    q = (query or "").strip()
    columns = datastore.get_columns()
    if not q or "meterid" not in columns:
        return {"query": q, "rows": [], "truncated": False}

    date_col = datastore.date_col
    has_customer_no = "customer_no" in columns
    match = f"CAST(meterid AS VARCHAR) ILIKE '%' || ? || '%'"
    params: List[Any] = [q]
    if has_customer_no:
        match += " OR CAST(customer_no AS VARCHAR) ILIKE '%' || ? || '%'"
        params.append(q)
    customer_no = "arg_max(CAST(customer_no AS VARCHAR), {d})".format(d=date_col) if has_customer_no else "NULL"
    utility = f"arg_max(CAST(utility AS VARCHAR), {date_col})" if "utility" in columns else "NULL"
    tariff = f"arg_max(CAST(tariff_type AS VARCHAR), {date_col})" if "tariff_type" in columns else "NULL"
    paymoney = "SUM(ocd_paymoney)" if "ocd_paymoney" in columns else "NULL"
    sql = f"""
        SELECT CAST(meterid AS VARCHAR) AS meterid, {customer_no} AS customer_no, {utility} AS utility,
               {tariff} AS tariff_type, MIN({date_col}) AS first_purchase, MAX({date_col}) AS last_purchase,
               COUNT(*) AS purchases, {paymoney} AS paymoney
        FROM {datastore.table_sql}
        WHERE meterid IS NOT NULL AND ({match})
        GROUP BY 1
        ORDER BY (CAST(meterid AS VARCHAR) = ?) DESC, last_purchase DESC, 1
        LIMIT ?
    """
    rows = datastore.run_query(sql, [*params, q, limit + 1])
    truncated = len(rows) > limit
    return {"query": q, "rows": rows[:limit], "truncated": truncated}


# --------------------------------------------------------------- account
def _month_labels(start: date, end: date) -> List[date]:
    out, d = [], start.replace(day=1)
    while d <= end:
        out.append(d)
        d = _shift_months(d, 1)
    return out


def _same_day_months_ago(d: date, months: int) -> date:
    shifted = _shift_months(d, -months)
    return shifted.replace(day=min(d.day, calendar.monthrange(shifted.year, shifted.month)[1]))


def _activity_status(last_purchase: Optional[date], data_end: Optional[date]) -> Dict[str, Any]:
    if not last_purchase or not data_end:
        return {"key": "unknown", "label": "Unknown", "months_since": None}
    months = max(0, months_between(last_purchase, data_end) - 1)
    if months <= ACTIVE_MONTHS:
        return {"key": "active", "label": "Active", "months_since": months}
    if months <= INACTIVE_MONTHS:
        return {"key": "dormant", "label": "Dormant", "months_since": months}
    return {"key": "inactive", "label": "Inactive", "months_since": months}


def _peer_where(columns: List[str], utility: Optional[str], tariff: Optional[str]):
    clauses, params = [], []
    if utility is not None and "utility" in columns:
        clauses.append("CAST(utility AS VARCHAR) = ?")
        params.append(str(utility))
    if tariff is not None and "tariff_type" in columns:
        clauses.append("CAST(tariff_type AS VARCHAR) = ?")
        params.append(str(tariff))
    return clauses, params


def customer_account(datastore, meterid: str, history: str = "24m") -> Optional[Dict[str, Any]]:
    """Everything the account page shows for one meter, or None when unknown."""
    columns = datastore.get_columns()
    if "meterid" not in columns:
        return None
    date_col = datastore.date_col
    table = datastore.table_sql
    cols = set(columns)
    meter = str(meterid).strip()

    # Latest attributes and lifetime totals.
    extras = detail_columns(columns)
    attr_sql = ", ".join(
        f'first("{c}" ORDER BY {date_col} DESC) FILTER (WHERE "{c}" IS NOT NULL) AS "{c}"'
        for c in ["customer_no", "utility", "tariff_type", *extras]
        if c in cols
    )
    energy = "SUM(ocd_energy)" if "ocd_energy" in cols else "NULL"
    paymoney = "SUM(ocd_paymoney)" if "ocd_paymoney" in cols else "NULL"
    sql = f"""
        SELECT COUNT(*) AS purchases, MIN({date_col}) AS first_purchase, MAX({date_col}) AS last_purchase,
               {energy} AS energy, {paymoney} AS paymoney{', ' + attr_sql if attr_sql else ''}
        FROM {table} WHERE {_meter_clause()}
    """
    head = datastore.fetch_one(sql, [meter])
    if not head or not head.get("purchases"):
        return None

    extent = datastore.data_extent()
    data_end: Optional[date] = extent.get("date_max") or head["last_purchase"]
    utility = head.get("utility")
    tariff = head.get("tariff_type")

    # Key figures: last 12 months of data versus the 12 months before.
    window_start = _shift_months(data_end, -11)
    kpi_params = FilterParams(start=window_start, end=data_end, selections={"meterid": [meter]})
    kpis = compute_kpis(datastore, kpi_params, date_col, columns)

    # Monthly series for the customer and the peer median (same district and account type).
    if history == "all":
        series_start = head["first_purchase"].replace(day=1)
    else:
        series_start = _shift_months(data_end, -23)
    labels = _month_labels(series_start, data_end)
    own = {
        r["m"]: r
        for r in datastore.run_query(
            f"""
            SELECT DATE_TRUNC('month', CAST({date_col} AS DATE))::DATE AS m,
                   {energy} AS kwh, {paymoney} AS paid, COUNT(*) AS n
            FROM {table} WHERE {_meter_clause()} AND CAST({date_col} AS DATE) BETWEEN ? AND ?
            GROUP BY 1
            """,
            [meter, series_start.isoformat(), data_end.isoformat()],
        )
    }
    peer_clauses, peer_params = _peer_where(columns, utility, tariff)
    peers_monthly: Dict[date, Dict[str, Any]] = {}
    peer_12: Dict[str, Any] = {"median_kwh": None, "median_paid": None, "customers": 0}
    if peer_clauses and "ocd_energy" in cols:
        peer_where = " AND ".join(peer_clauses)
        peers_monthly = {
            r["m"]: r
            for r in datastore.run_query(
                f"""
                WITH per_meter AS (
                    SELECT DATE_TRUNC('month', CAST({date_col} AS DATE))::DATE AS m, meterid,
                           SUM(ocd_energy) AS kwh
                    FROM {table}
                    WHERE {peer_where} AND CAST({date_col} AS DATE) BETWEEN ? AND ?
                    GROUP BY 1, 2
                )
                SELECT m, MEDIAN(kwh) AS kwh, COUNT(*) AS n FROM per_meter GROUP BY 1
                """,
                [*peer_params, series_start.isoformat(), data_end.isoformat()],
            )
        }
        row = datastore.fetch_one(
            f"""
            WITH per_meter AS (
                SELECT meterid, SUM(ocd_energy) AS kwh, {paymoney} AS paid
                FROM {table}
                WHERE {peer_where} AND CAST({date_col} AS DATE) BETWEEN ? AND ?
                GROUP BY 1
            )
            SELECT MEDIAN(kwh) AS median_kwh, MEDIAN(paid) AS median_paid, COUNT(*) AS customers FROM per_meter
            """,
            [*peer_params, window_start.isoformat(), data_end.isoformat()],
        )
        if row:
            peer_12 = {
                "median_kwh": None if row["median_kwh"] is None else float(row["median_kwh"]),
                "median_paid": None if row["median_paid"] is None else float(row["median_paid"]),
                "customers": int(row["customers"] or 0),
            }

    series = {
        "labels": [d.strftime("%Y-%m") for d in labels],
        "kwh": [round(float(own[d]["kwh"] or 0), 2) if d in own else 0.0 for d in labels],
        "paid": [round(float(own[d]["paid"] or 0), 2) if d in own else 0.0 for d in labels],
        "peer_kwh": [round(float(peers_monthly[d]["kwh"]), 2) if d in peers_monthly else None for d in labels],
    }

    cur = kpis["current"]
    own_12 = cur.get("energy")
    peer_ratio = (
        own_12 / peer_12["median_kwh"] if own_12 is not None and peer_12["median_kwh"] else None
    )

    # Signals: simple, explainable rules (indicators for a visit, not evidence).
    status = _activity_status(head["last_purchase"], data_end)
    signals: List[Dict[str, str]] = []
    if status["key"] in ("dormant", "inactive"):
        signals.append({
            "level": "warning",
            "title": f"No purchases for {status['months_since']} months",
            "detail": f"Last purchase on {head['last_purchase'].strftime('%-d %b %Y')}; the data runs to "
                      f"{data_end.strftime('%-d %b %Y')}.",
        })

    # Last 3 full months versus the same 3 months a year earlier.
    recent_start = _shift_months(data_end, -2)
    year_ago_start = _shift_months(recent_start, -12)
    year_ago_end = _same_day_months_ago(data_end, 12)
    if "ocd_energy" in cols and head["first_purchase"] < year_ago_start:
        row = datastore.fetch_one(
            f"""
            SELECT SUM(CASE WHEN CAST({date_col} AS DATE) >= ? THEN ocd_energy END) AS recent,
                   SUM(CASE WHEN CAST({date_col} AS DATE) BETWEEN ? AND ? THEN ocd_energy END) AS year_ago
            FROM {table} WHERE {_meter_clause()}
            """,
            [recent_start.isoformat(), year_ago_start.isoformat(), year_ago_end.isoformat(), meter],
        )
        recent = float(row.get("recent") or 0) if row else 0.0
        year_ago = float(row.get("year_ago") or 0) if row else 0.0
        if year_ago > 0 and recent / year_ago < DROP_THRESHOLD and status["key"] == "active":
            change = pct_change(recent, year_ago)
            signals.append({
                "level": "warning",
                "title": f"Consumption down {abs(change):.0f}% on a year ago",
                "detail": f"{recent:,.0f} kWh bought in the last 3 months against {year_ago:,.0f} kWh in the same "
                          f"months a year earlier.",
            })

    if peer_ratio is not None and status["key"] == "active" and peer_ratio < PEER_LOW_THRESHOLD:
        signals.append({
            "level": "warning",
            "title": f"Buying {peer_ratio * 100:.0f}% of what similar customers buy",
            "detail": f"{own_12:,.0f} kWh in the last 12 months against a median of {peer_12['median_kwh']:,.0f} kWh "
                      f"for {tariff or 'similar'} customers in {utility or 'the same district'}.",
        })

    if not signals:
        signals.append({
            "level": "ok",
            "title": "Nothing to flag",
            "detail": "Purchases are recent and in line with similar customers." if peer_ratio is not None
                      else "Purchases are recent.",
        })

    purchases = datastore.run_query(
        f"SELECT * FROM {table} WHERE {_meter_clause()} ORDER BY {date_col} DESC LIMIT ?",
        [meter, PURCHASES_LIMIT],
    )

    return {
        "meterid": meter,
        "customer_no": head.get("customer_no"),
        "utility": utility,
        "tariff_type": tariff,
        "details": [(pretty_label(c), head.get(c)) for c in extras if head.get(c) not in (None, "")],
        "purchases": int(head["purchases"]),
        "first_purchase": head["first_purchase"],
        "last_purchase": head["last_purchase"],
        "energy": None if head.get("energy") is None else float(head["energy"]),
        "paymoney": None if head.get("paymoney") is None else float(head["paymoney"]),
        "status": status,
        "data_end": data_end,
        "window": {"start": window_start, "end": data_end},
        "kpis": kpis,
        "peer": {**peer_12, "ratio": peer_ratio},
        "series": series,
        "history": history,
        "signals": signals,
        "recent_purchases": purchases,
        "recent_limit": min(PURCHASES_LIMIT, int(head["purchases"])),
    }


__all__ = ["search_customers", "customer_account", "detail_columns", "pretty_label"]

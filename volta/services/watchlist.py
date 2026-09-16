"""Watch list: every meter scored with the same rules as the account page.

The list is materialised in a DuckDB table (``watchlist``) built from the
dataset in one pass and rebuilt whenever the dataset changes, so viewing,
filtering and exporting it costs almost nothing. Inspection outcomes that
officers record live in a second table (``inspections``) that survives data
uploads.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from .customers import ACTIVE_MONTHS, DROP_THRESHOLD, INACTIVE_MONTHS, PEER_LOW_THRESHOLD
from .datastore import _sql_literal
from .kpis import _shift_months

# Rule weights: a dormant meter that used to buy is the strongest bypass indicator;
# a meter silent for over a year more often means a customer who left.
WEIGHT_DORMANT = 3
WEIGHT_INACTIVE = 1
WEIGHT_DROP = 2
WEIGHT_LOW_VS_PEERS = 2

OUTCOMES = [
    ("bypass", "Bypass or tampering found"),
    ("nothing", "Nothing found"),
    ("faulty", "Meter faulty"),
    ("vacant", "Property vacant or disconnected"),
    ("other", "Other (see note)"),
]
OUTCOME_LABELS = dict(OUTCOMES)
SIGNAL_LABELS = {"dormant": "No recent purchases", "inactive": "Inactive over a year", "drop": "Consumption drop",
                 "low": "Low versus peers"}
SORTS = {"score": "score DESC, shortfall_kwh DESC", "shortfall": "shortfall_kwh DESC, score DESC",
         "last_purchase": "last_purchase ASC"}
PAGE_SIZE = 100


def _same_day_months_ago(d: date, months: int) -> date:
    shifted = _shift_months(d, -months)
    return shifted.replace(day=min(d.day, calendar.monthrange(shifted.year, shifted.month)[1]))


# --------------------------------------------------------------- build
def ensure_inspections_table(datastore) -> None:
    datastore.execute(
        """
        CREATE TABLE IF NOT EXISTS inspections (
            id          BIGINT,
            meterid     VARCHAR,
            outcome     VARCHAR,
            note        VARCHAR,
            recorded_by VARCHAR,
            recorded_at TIMESTAMP
        )
        """
    )


def _table_exists(datastore, name: str) -> bool:
    return datastore.fetch_one("SELECT 1 AS x FROM information_schema.tables WHERE table_name = ?", [name]) is not None


def watchlist_is_current(datastore) -> bool:
    if not _table_exists(datastore, "watchlist_meta"):
        return False
    extent = datastore.data_extent()
    meta = datastore.fetch_one("SELECT date_max, n_rows FROM watchlist_meta")
    return bool(meta) and meta["date_max"] == extent.get("date_max") and int(meta["n_rows"]) == int(extent.get("rows") or 0)


def build_watchlist(datastore) -> int:
    """(Re)build the watchlist table from the dataset; returns the number of meters."""
    columns = set(datastore.get_columns())
    extent = datastore.data_extent()
    data_end: Optional[date] = extent.get("date_max")
    ensure_inspections_table(datastore)
    if "meterid" not in columns or not data_end:
        datastore.execute("CREATE OR REPLACE TABLE watchlist AS SELECT CAST(NULL AS VARCHAR) AS meterid WHERE FALSE")
        datastore.execute("CREATE OR REPLACE TABLE watchlist_meta AS SELECT CAST(NULL AS DATE) AS date_max, 0::BIGINT AS n_rows, now()::TIMESTAMP AS built_at")
        return 0

    date_col = datastore.date_col
    t = datastore.table_sql
    d = f"CAST({date_col} AS DATE)"
    energy = "ocd_energy" if "ocd_energy" in columns else "0"
    w12 = _shift_months(data_end, -11)          # last 12 months to date
    w24 = _shift_months(data_end, -23)          # the 12 months before those
    r3 = _shift_months(data_end, -2)            # last 3 months to date
    y3s, y3e = _shift_months(r3, -12), _same_day_months_ago(data_end, 12)
    customer_no = f"arg_max(CAST(customer_no AS VARCHAR), {d})" if "customer_no" in columns else "NULL"
    utility = f"arg_max(CAST(utility AS VARCHAR), {d})" if "utility" in columns else "NULL"
    tariff = f"arg_max(CAST(tariff_type AS VARCHAR), {d})" if "tariff_type" in columns else "NULL"

    def lit(day: date) -> str:
        return f"DATE {_sql_literal(day.isoformat())}"

    end_lit, w12_lit, w24_lit, r3_lit, y3s_lit, y3e_lit = (lit(x) for x in (data_end, w12, w24, r3, y3s, y3e))
    months_since = f"date_diff('month', m.last_purchase, {end_lit})"
    sql = f"""
        CREATE OR REPLACE TABLE watchlist AS
        WITH per_meter AS (
            SELECT CAST(meterid AS VARCHAR) AS meterid,
                   {customer_no} AS customer_no, {utility} AS utility, {tariff} AS tariff_type,
                   MIN({d}) AS first_purchase, MAX({d}) AS last_purchase, COUNT(*) AS purchases,
                   COALESCE(SUM(CASE WHEN {d} >= {w12_lit} THEN {energy} END), 0) AS kwh_12,
                   COALESCE(SUM(CASE WHEN {d} >= {w24_lit} AND {d} < {w12_lit} THEN {energy} END), 0) AS kwh_prev_12,
                   COALESCE(SUM(CASE WHEN {d} >= {r3_lit} THEN {energy} END), 0) AS kwh_3,
                   COALESCE(SUM(CASE WHEN {d} BETWEEN {y3s_lit} AND {y3e_lit} THEN {energy} END), 0) AS kwh_3_year_ago
            FROM {t}
            WHERE meterid IS NOT NULL
            GROUP BY 1
        ),
        peers AS (
            SELECT utility, tariff_type, MEDIAN(kwh_12) AS peer_kwh_12, COUNT(*) AS peers
            FROM per_meter
            WHERE last_purchase >= {w12_lit}
            GROUP BY 1, 2
        ),
        scored AS (
            SELECT m.*, p.peer_kwh_12, COALESCE(p.peers, 0) AS peers,
                   {months_since} AS months_since,
                   ({months_since} > {ACTIVE_MONTHS} AND {months_since} <= {INACTIVE_MONTHS}) AS is_dormant,
                   ({months_since} > {INACTIVE_MONTHS}) AS is_inactive,
                   ({months_since} <= {ACTIVE_MONTHS} AND m.first_purchase < {y3s_lit} AND m.kwh_3_year_ago > 0
                    AND m.kwh_3 / m.kwh_3_year_ago < {DROP_THRESHOLD}) AS is_drop,
                   ({months_since} <= {ACTIVE_MONTHS} AND COALESCE(p.peer_kwh_12, 0) > 0
                    AND m.kwh_12 / p.peer_kwh_12 < {PEER_LOW_THRESHOLD}) AS is_low
            FROM per_meter m
            LEFT JOIN peers p ON p.utility IS NOT DISTINCT FROM m.utility AND p.tariff_type IS NOT DISTINCT FROM m.tariff_type
        )
        SELECT *,
               (CASE WHEN is_dormant THEN {WEIGHT_DORMANT} ELSE 0 END
                + CASE WHEN is_inactive THEN {WEIGHT_INACTIVE} ELSE 0 END
                + CASE WHEN is_drop THEN {WEIGHT_DROP} ELSE 0 END
                + CASE WHEN is_low THEN {WEIGHT_LOW_VS_PEERS} ELSE 0 END) AS score,
               GREATEST(0, kwh_prev_12 - kwh_12, COALESCE(peer_kwh_12, 0) - kwh_12) AS shortfall_kwh,
               CASE WHEN kwh_prev_12 > 0 THEN (kwh_12 - kwh_prev_12) / kwh_prev_12 * 100 END AS change_pct,
               CASE WHEN COALESCE(peer_kwh_12, 0) > 0 THEN kwh_12 / peer_kwh_12 * 100 END AS peer_pct
        FROM scored
    """
    datastore.execute(sql)
    datastore.execute(
        f"CREATE OR REPLACE TABLE watchlist_meta AS SELECT {end_lit} AS date_max, "
        f"{int(extent.get('rows') or 0)}::BIGINT AS n_rows, now()::TIMESTAMP AS built_at"
    )
    row = datastore.fetch_one("SELECT COUNT(*) AS n FROM watchlist")
    return int(row["n"]) if row else 0


def ensure_watchlist(datastore) -> None:
    if not watchlist_is_current(datastore):
        build_watchlist(datastore)


# --------------------------------------------------------------- query
def _where(filters: Dict[str, Any]):
    clauses, params = ["score > 0"], []
    if filters.get("utility"):
        clauses.append("w.utility = ?")
        params.append(filters["utility"])
    if filters.get("tariff_type"):
        clauses.append("w.tariff_type = ?")
        params.append(filters["tariff_type"])
    signal = filters.get("signal")
    if signal in ("dormant", "inactive", "drop", "low"):
        clauses.append(f"w.is_{signal}")
    inspected = filters.get("inspected")
    if inspected == "no":
        clauses.append("i.outcome IS NULL")
    elif inspected == "yes":
        clauses.append("i.outcome IS NOT NULL")
    return " AND ".join(clauses), params


_LATEST_INSPECTION = """
    SELECT meterid, outcome, note, recorded_by, recorded_at
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY meterid ORDER BY recorded_at DESC, id DESC) AS rn FROM inspections)
    WHERE rn = 1
"""


def query_watchlist(datastore, filters: Dict[str, Any], sort: str = "score", offset: int = 0,
                    limit: Optional[int] = PAGE_SIZE) -> Dict[str, Any]:
    ensure_watchlist(datastore)
    where, params = _where(filters)
    order = SORTS.get(sort, SORTS["score"])
    base = f"FROM watchlist w LEFT JOIN ({_LATEST_INSPECTION}) i ON i.meterid = w.meterid WHERE {where}"
    total = datastore.fetch_one(f"SELECT COUNT(*) AS n {base}", params)
    sql = f"""
        SELECT w.*, i.outcome AS inspection_outcome, i.recorded_at AS inspection_at, i.recorded_by AS inspection_by
        {base} ORDER BY {order}, w.meterid
    """
    if limit is not None:
        sql += f" LIMIT {int(limit)} OFFSET {int(offset)}"
    rows = datastore.run_query(sql, params)
    for r in rows:
        r["signals"] = [k for k in ("dormant", "inactive", "drop", "low") if r.get(f"is_{k}")]
        r["outcome_label"] = OUTCOME_LABELS.get(r.get("inspection_outcome") or "", r.get("inspection_outcome"))
    return {"rows": rows, "total": int(total["n"]) if total else 0, "offset": offset, "limit": limit}


def watchlist_summary(datastore) -> Dict[str, Any]:
    ensure_watchlist(datastore)
    row = datastore.fetch_one(
        """
        SELECT COUNT(*) AS meters, SUM(CASE WHEN score > 0 THEN 1 ELSE 0 END) AS flagged,
               SUM(CASE WHEN is_dormant THEN 1 ELSE 0 END) AS dormant, SUM(CASE WHEN is_inactive THEN 1 ELSE 0 END) AS inactive,
               SUM(CASE WHEN is_drop THEN 1 ELSE 0 END) AS drop_, SUM(CASE WHEN is_low THEN 1 ELSE 0 END) AS low
        FROM watchlist
        """
    ) or {}
    meta = datastore.fetch_one("SELECT date_max, built_at FROM watchlist_meta") or {}
    insp = datastore.fetch_one("SELECT COUNT(DISTINCT meterid) AS n FROM inspections") or {}
    return {
        "meters": int(row.get("meters") or 0), "flagged": int(row.get("flagged") or 0),
        "dormant": int(row.get("dormant") or 0), "inactive": int(row.get("inactive") or 0),
        "drop": int(row.get("drop_") or 0), "low": int(row.get("low") or 0),
        "data_end": meta.get("date_max"), "built_at": meta.get("built_at"),
        "inspected": int(insp.get("n") or 0),
    }


def districts_and_tariffs(datastore):
    ensure_watchlist(datastore)
    d = [r["v"] for r in datastore.run_query("SELECT DISTINCT utility AS v FROM watchlist WHERE utility IS NOT NULL ORDER BY 1")]
    t = [r["v"] for r in datastore.run_query("SELECT DISTINCT tariff_type AS v FROM watchlist WHERE tariff_type IS NOT NULL ORDER BY 1")]
    return d, t


# ---------------------------------------------------------- inspections
def add_inspection(datastore, meterid: str, outcome: str, note: str, recorded_by: str) -> None:
    if outcome not in OUTCOME_LABELS:
        raise ValueError("Unknown outcome")
    ensure_inspections_table(datastore)
    row = datastore.fetch_one("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM inspections")
    datastore.execute(
        "INSERT INTO inspections (id, meterid, outcome, note, recorded_by, recorded_at) VALUES (?, ?, ?, ?, ?, ?)",
        [int(row["next_id"]), str(meterid), outcome, (note or "").strip()[:500], (recorded_by or "").strip()[:80],
         datetime.now().replace(microsecond=0)],
    )


def inspections_for(datastore, meterid: str) -> List[Dict[str, Any]]:
    ensure_inspections_table(datastore)
    rows = datastore.run_query(
        "SELECT outcome, note, recorded_by, recorded_at FROM inspections WHERE meterid = ? ORDER BY recorded_at DESC, id DESC",
        [str(meterid)],
    )
    for r in rows:
        r["outcome_label"] = OUTCOME_LABELS.get(r["outcome"], r["outcome"])
    return rows


__all__ = ["OUTCOMES", "OUTCOME_LABELS", "SIGNAL_LABELS", "SORTS", "PAGE_SIZE", "build_watchlist", "ensure_watchlist",
           "query_watchlist", "watchlist_summary", "districts_and_tariffs", "add_inspection", "inspections_for"]

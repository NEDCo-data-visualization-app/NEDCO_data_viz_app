"""PowerPoint export of the overview for the current filters."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List

from flask import Response, current_app, request

from ...services.export_pptx import build_brief
from ...services.kpis import compute_kpis
from ..auth import effective_public_mode
from . import bp, get_datastore
from .helpers import build_params

FILTER_NAMES = {"utility": "District", "tariff_type": "Account type", "meterid": "Meter number"}


def _monthly(datastore, clause: str, sql_params, columns: List[str], date_col: str) -> Dict[str, Any]:
    cols = set(columns)
    energy = "SUM(ocd_energy)" if "ocd_energy" in cols else "0"
    paymoney = "SUM(ocd_paymoney)" if "ocd_paymoney" in cols else "0"
    sql = f"""
        SELECT strftime(date_trunc('month', CAST({date_col} AS DATE)), '%Y-%m') AS bucket,
               COALESCE({energy}, 0) AS energy, COALESCE({paymoney}, 0) AS paymoney
        FROM {datastore.table_sql}
        {('WHERE ' + clause) if clause else ''}
        GROUP BY 1 ORDER BY 1
    """
    rows = datastore.run_query(sql, sql_params)
    return {
        "labels": [r["bucket"] for r in rows],
        "energy": [float(r["energy"] or 0) for r in rows],
        "paymoney": [float(r["paymoney"] or 0) for r in rows],
    }


def _districts(datastore, clause: str, sql_params, columns: List[str]) -> List[Dict[str, Any]]:
    cols = set(columns)
    if "utility" not in cols:
        return []
    energy = "SUM(ocd_energy)" if "ocd_energy" in cols else "0"
    paymoney = "SUM(ocd_paymoney)" if "ocd_paymoney" in cols else "0"
    customers = "COUNT(DISTINCT meterid)" if "meterid" in cols else "0"
    sql = f"""
        SELECT CAST(utility AS VARCHAR) AS label, COALESCE({energy}, 0) AS energy,
               COALESCE({paymoney}, 0) AS paymoney, {customers} AS customers
        FROM {datastore.table_sql}
        WHERE utility IS NOT NULL{(' AND ' + clause) if clause else ''}
        GROUP BY 1 ORDER BY energy DESC, 1
    """
    return [
        {"label": r["label"], "energy": float(r["energy"] or 0), "paymoney": float(r["paymoney"] or 0),
         "customers": int(r["customers"] or 0)}
        for r in datastore.run_query(sql, sql_params)
    ]


def _mix(datastore, clause: str, sql_params, columns: List[str]) -> Dict[str, List]:
    cols = set(columns)
    if "tariff_type" not in cols:
        return {"labels": [], "customers": [], "energy": []}
    energy = "SUM(ocd_energy)" if "ocd_energy" in cols else "0"
    customers = "COUNT(DISTINCT meterid)" if "meterid" in cols else "COUNT(*)"
    sql = f"""
        SELECT CAST(tariff_type AS VARCHAR) AS label, {customers} AS customers, COALESCE({energy}, 0) AS energy
        FROM {datastore.table_sql}
        WHERE tariff_type IS NOT NULL{(' AND ' + clause) if clause else ''}
        GROUP BY 1 ORDER BY customers DESC, 1
    """
    rows = datastore.run_query(sql, sql_params)
    return {
        "labels": [r["label"] for r in rows],
        "customers": [int(r["customers"] or 0) for r in rows],
        "energy": [float(r["energy"] or 0) for r in rows],
    }


def _filters_label(params, public: bool) -> str:
    parts = []
    for col, values in (params.selections or {}).items():
        if not values:
            continue
        if col.lower() == "meterid" and public:
            continue
        name = FILTER_NAMES.get(col.lower(), col)
        shown = ", ".join(values[:6]) + (f" and {len(values) - 6} more" if len(values) > 6 else "")
        parts.append(f"{name}: {shown}")
    return " · ".join(parts) if parts else "All districts and account types"


@bp.route("/export/pptx", methods=["GET"])
def export_pptx():
    datastore = get_datastore()
    columns = datastore.get_columns()
    date_col = current_app.config.get("DATE_COL", "od_date")
    public = effective_public_mode()
    params = build_params(request.args, base_columns=columns)
    if columns and (params.selections or params.start or params.end):
        clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=columns)
    else:
        clause, sql_params = "", []

    extent = datastore.data_extent()
    kpis = compute_kpis(datastore, params, date_col, columns) if columns else {
        "current": {"purchases": 0}, "previous": None, "period": {"start": None, "end": None, "months": 0}, "deltas": {},
    }
    period = kpis["period"]
    start, end = period.get("start"), period.get("end")
    if start and end:
        period_label = f"{start.strftime('%-d %b %Y')} to {end.strftime('%-d %b %Y')} ({period['months']} month{'s' if period['months'] != 1 else ''})"
    else:
        period_label = "No transactions match these filters"

    data = {
        "kpis": kpis,
        "period_label": period_label,
        "filters_label": _filters_label(params, public),
        "data_through": extent.get("date_max_label") or "—",
        "generated": datetime.now().strftime("%-d %b %Y"),
        "monthly": _monthly(datastore, clause, sql_params, columns, date_col) if columns else {"labels": [], "energy": [], "paymoney": []},
        "districts": _districts(datastore, clause, sql_params, columns) if columns else [],
        "mix": _mix(datastore, clause, sql_params, columns) if columns else {"labels": [], "customers": [], "energy": []},
    }
    payload = build_brief(data)
    stamp = f"{start.isoformat()}_{end.isoformat()}" if start and end else date.today().isoformat()
    return Response(
        payload,
        mimetype="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        headers={"Content-Disposition": f'attachment; filename="nedco_brief_{stamp}.pptx"'},
    )

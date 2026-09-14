"""Healthcheck endpoint."""

from __future__ import annotations

from flask import current_app, jsonify

from . import bp, get_datastore


@bp.route("/health", methods=["GET"])
def health():
    datastore = get_datastore()
    try:
        cols = datastore.get_columns()
        rows = 0
        if cols:
            row = datastore.fetch_one(f"SELECT COUNT(*) AS n FROM {datastore.table_sql}")
            rows = int(row["n"]) if row else 0
        return jsonify({"ok": True, "rows": rows, "cols": len(cols), "table": datastore.table}), 200
    except Exception as exc:  # noqa: BLE001
        current_app.logger.exception("Healthcheck failed")
        return jsonify({"ok": False, "error": str(exc)}), 500

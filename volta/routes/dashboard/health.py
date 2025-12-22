"""Healthcheck endpoint (DuckDB + Parquet, persistent connection)."""

from __future__ import annotations
from flask import current_app, jsonify
from . import bp, get_datastore

@bp.route("/health", methods=["GET"])
def health():
    datastore = get_datastore()
    try:
        # Probe first row to count columns
        probe_gen = datastore.run_query(
            f'SELECT * FROM "{current_app.config["PARQUET_PATH"]}" LIMIT 1',
            fetch_all=False
        )
        first_row = next(probe_gen, None)
        cols = len(first_row) if first_row else 0

        # Count total rows (single aggregate, safe)
        count_rows = datastore.run_query(
            f'SELECT COUNT(*) AS n FROM "{current_app.config["PARQUET_PATH"]}"'
        )
        rows = int(count_rows[0]["n"]) if count_rows else 0

        return jsonify({"ok": True, "rows": rows, "cols": cols}), 200

    except Exception as exc:  # defensive logging
        current_app.logger.exception("Healthcheck failed")
        return jsonify({"ok": False, "error": str(exc)}), 500

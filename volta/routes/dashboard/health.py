"""Healthcheck endpoint (DuckDB + Parquet, no pandas)."""

from __future__ import annotations

from flask import current_app, jsonify

from . import bp, get_datastore


@bp.route("/health", methods=["GET"])
def health():
    datastore = get_datastore()
    try:
        # Probe first row to count columns
        probe = datastore.run_query(f"SELECT * FROM '{current_app.config["PARQUET_PATH"]}' LIMIT 1")
        cols = len(probe[0]) if probe else 0

        # Count total rows
        count_rows = datastore.run_query(f"SELECT COUNT(*) AS n FROM '{current_app.config["PARQUET_PATH"]}'")
        rows = int(count_rows[0]["n"]) if count_rows else 0

        return jsonify({"ok": True, "rows": rows, "cols": cols}), 200
    except Exception as exc:  # pragma: no cover - defensive logging path
        current_app.logger.exception("Healthcheck failed")
        return jsonify({"ok": False, "error": str(exc)}), 500

"""Healthcheck endpoint.

Hosted platforms poll this every few seconds and restart the service when it
stops answering, so it must never wait on a long-running query or ingest.
"""

from __future__ import annotations

from flask import current_app, jsonify

from . import bp, get_datastore


@bp.route("/health", methods=["GET"])
def health():
    try:
        return jsonify(get_datastore().health()), 200
    except Exception as exc:  # noqa: BLE001
        current_app.logger.exception("Healthcheck failed")
        return jsonify({"ok": False, "error": str(exc)}), 500

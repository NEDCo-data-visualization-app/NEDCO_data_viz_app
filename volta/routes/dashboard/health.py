"""Healthcheck endpoint."""

from __future__ import annotations

from flask import current_app, jsonify

from . import bp, get_datastore


@bp.route("/health", methods=["GET"])
def health():
    datastore = get_datastore()
    try:
        df = datastore.get(copy=False)
        return (
            jsonify(
                {
                    "ok": True,
                    "rows": int(len(df)),
                    "cols": int(len(df.columns)),
                }
            ),
            200,
        )
    except Exception as exc:  # pragma: no cover - defensive logging path
        current_app.logger.exception("Healthcheck failed")
        return jsonify({"ok": False, "error": str(exc)}), 500
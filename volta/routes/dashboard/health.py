"""Healthcheck endpoint."""

from __future__ import annotations

from flask import current_app, jsonify

from . import bp, get_datastore


@bp.route("/health", methods=["GET"])
def health():
    datastore = get_datastore()
    try:
        # Query just to count rows and columns directly in BigQuery
        row_count_query = f"SELECT COUNT(*) AS n FROM `{datastore.TABLE_NAME}`"
        col_count_query = f"""
            SELECT COUNT(*) AS n
            FROM `{datastore.TABLE_NAME}` 
            LIMIT 1
        """

        # Get number of rows
        df_rows = datastore.run_query(row_count_query)
        rows = int(df_rows.iloc[0]["n"]) if df_rows is not None else 0

        # Get number of columns (run a single-row query and count columns)
        df_cols = datastore.run_query(col_count_query)
        cols = len(df_cols.columns) if df_cols is not None else 0

        return jsonify({"ok": True, "rows": rows, "cols": cols}), 200

    except Exception as exc:  # defensive logging
        current_app.logger.exception("Healthcheck failed")
        return jsonify({"ok": False, "error": str(exc)}), 500

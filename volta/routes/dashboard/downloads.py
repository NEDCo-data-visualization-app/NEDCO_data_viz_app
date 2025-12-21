"""Download endpoints for dashboard (DuckDB + Parquet, no pandas)."""

from __future__ import annotations

from datetime import datetime
from flask import Response, current_app, request

from . import bp, get_datastore
from .helpers import build_params


@bp.route("/download-csv", methods=["GET"])
def download_csv():
    """Download filtered dataset as CSV, streamed directly from DuckDB."""
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()

    # Build FilterParams from request args (no DataFrame needed)
    params = build_params(request.args, None)

    clause, sql_params = params.to_sql_where(
        date_col=date_col,
        available_columns=None,  # allow any column
    )

    sql = f"""
        SELECT *
        FROM '{current_app.config["PARQUET_PATH"]}'
        WHERE {clause}
        ORDER BY {date_col}
    """

    def generate():
        """Stream CSV rows directly from DuckDB."""
        con = datastore._connect()
        try:
            cur = con.execute(sql, sql_params)

            # Header
            cols = [c[0] for c in cur.description]
            yield ",".join(cols) + "\n"

            # Rows
            for row in cur.fetchall():
                # Convert each value to string, empty string for None
                yield ",".join("" if v is None else str(v) for v in row) + "\n"
        finally:
            con.close()

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"export_{ts}.csv"

    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )

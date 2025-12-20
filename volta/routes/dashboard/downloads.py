"""Download endpoints for dashboard."""

from __future__ import annotations

import io
from datetime import datetime

from flask import Response, current_app, request

from . import bp, get_datastore
from .helpers import build_params


@bp.route("/download-csv", methods=["GET"])
def download_csv():
    """Download the filtered dataset as CSV directly from BigQuery."""

    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    
    # Build filter params from request (but without a local DataFrame)
    params = build_params(request.args, available_columns=None)

    # Generate WHERE clause for SQL based on filters
    clause, sql_params = params.to_sql_where(
        date_col=date_col,
        available_columns=None,  # None => use only columns in SQL table
        param_style="named",
    )

    sql = f"""
        SELECT *
        FROM `{datastore.TABLE_NAME}`
        WHERE {clause}
        ORDER BY {date_col} DESC
    """

    df = datastore.run_query(sql, sql_params)

    # Convert result directly to CSV
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"export_{ts}.csv"

    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )

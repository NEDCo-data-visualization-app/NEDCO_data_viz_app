"""Download endpoints for dashboard."""

from __future__ import annotations

import io
from datetime import datetime

from flask import Response, current_app, request

from . import bp, get_datastore
from .helpers import build_params


@bp.route("/download-csv", methods=["GET"])
def download_csv():
    """Download the entire filtered dataset as CSV."""
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    base = datastore.get(copy=False)

    params = build_params(request.args, base)
    filtered = params.apply(base, date_col)

    buf = io.StringIO()
    filtered.to_csv(buf, index=False)
    buf.seek(0)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"export_{ts}.csv"

    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
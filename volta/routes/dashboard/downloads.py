"""CSV export of the filtered dataset, streamed from DuckDB."""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
from typing import Iterable, Iterator, List

from flask import Response, current_app, request

from . import bp, get_datastore
from .helpers import build_params


def stream_csv(chunks: Iterable[tuple]) -> Iterator[str]:
    """Turn (columns, rows) chunks from DataStore.stream_query into CSV text."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    header_written = False
    for cols, rows in chunks:
        if not header_written:
            writer.writerow(cols)
            header_written = True
        writer.writerows(["" if v is None else v for v in row] for row in rows)
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)


def csv_response(chunks: Iterable[tuple], filename: str) -> Response:
    return Response(
        stream_csv(chunks),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@bp.route("/download-csv", methods=["GET"])
def download_csv():
    date_col = current_app.config["DATE_COL"]
    datastore = get_datastore()
    columns: List[str] = datastore.get_columns()
    params = build_params(request.args, base_columns=columns)
    clause, sql_params = params.to_sql_where(date_col=date_col, available_columns=columns)

    sql = f"SELECT * FROM {datastore.table_sql} WHERE {clause} ORDER BY {date_col}"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return csv_response(datastore.stream_query(sql, sql_params), f"export_{ts}.csv")

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
    columns = datastore.get_columns()

    # Build FilterParams from request args
    params = build_params(request.args, base_columns=columns)

    clause, sql_params = params.to_sql_where(
        date_col=date_col,
        available_columns=None,  # allow any column
    )

    sql = f'''
        SELECT *
        FROM "{current_app.config["PARQUET_PATH"]}"
    '''
    if clause:
        sql += f" WHERE {clause}"
    sql += f" ORDER BY {date_col}"

    def generate():
        """Stream CSV rows directly from DuckDB using generator."""
        try:
            rows = datastore.run_query(sql, sql_params, fetch_all=False)  # generator

            # Header
            first_row = next(rows, None)
            if not first_row:
                return  # nothing to yield
            cols = list(first_row.keys())
            yield ",".join(cols) + "\n"

            # First row
            yield ",".join("" if v is None else str(v) for v in first_row.values()) + "\n"

            # Remaining rows
            for row in rows:
                yield ",".join("" if v is None else str(v) for v in row.values()) + "\n"
        except Exception as e:
            # Optional: log error here
            pass

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"export_{ts}.csv"

    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )

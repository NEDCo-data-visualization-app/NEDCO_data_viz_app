from flask import Blueprint, request, render_template, redirect, url_for, current_app
from pathlib import Path
from werkzeug.utils import secure_filename
import logging

upload_bp = Blueprint("upload", __name__, template_folder="../../templates")

# Use the configured uploads location (default set in Config.CSV_GLOB = "data/uploads/*.csv")
def _uploads_dir() -> Path:
    glob_pat = current_app.config.get("CSV_GLOB", "data/uploads/*.csv")
    # strip the trailing pattern to get the directory
    p = Path(glob_pat)
    return (p.parent if p.suffix else Path(glob_pat)).resolve()

ALLOWED_EXTENSIONS = {"csv"}
logger = logging.getLogger("volta.upload")

def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

@upload_bp.route("/upload", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        if "file" not in request.files:
            logger.warning("No file part in request")
            return redirect(request.url)

        file = request.files["file"]
        if file.filename == "":
            logger.warning("No file selected")
            return redirect(request.url)

        if not allowed_file(file.filename):
            logger.warning("Unsupported file format (only .csv allowed now)")
            return redirect(request.url)

        try:
            # Ensure uploads dir exists
            uploads_dir = _uploads_dir()
            uploads_dir.mkdir(parents=True, exist_ok=True)

            # Save CSV to configured uploads dir
            filename = secure_filename(file.filename)
            filepath = uploads_dir / filename
            file.save(str(filepath))
            logger.info("Saved upload to %s", filepath)

            # Materialize CSVs -> DuckDB (rebuild the prod.sales table)
            datastore = current_app.extensions["datastore"]
            datastore.rebuild_from_csv()

            # Done: we no longer write Parquet or modify DATA_PATH
            logger.info("Rebuilt DuckDB from CSV uploads successfully")
            return redirect(url_for("dashboard.index"))

        except Exception as e:
            logger.error("Error processing upload: %s", e, exc_info=True)
            return redirect(request.url)

    return render_template("upload.html")

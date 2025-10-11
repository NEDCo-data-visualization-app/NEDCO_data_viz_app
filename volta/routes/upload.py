from flask import Blueprint, request, render_template, redirect, url_for, current_app, Response
import os
from pathlib import Path
from werkzeug.utils import secure_filename
import logging
import pandas as pd

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
            logger.warning("Unsupported file format")
            return redirect(request.url)

        try:
            uploads_dir = _uploads_dir()
            uploads_dir.mkdir(parents=True, exist_ok=True)
            filename = secure_filename(file.filename)
            filepath = uploads_dir / filename
            file.save(str(filepath))
            logger.info("Saved upload to %s", filepath)

            df = pd.read_csv(filepath)
            datastore = current_app.extensions["datastore"]
            datastore.set_df(df) 
            logger.info("Uploaded CSV loaded into DataStore successfully")

            os.remove(filepath)
            logger.info("Temporary uploaded CSV removed from server")
            return redirect(url_for("dashboard.index"))

        except Exception as e:
            logger.error("Error processing upload: %s", e, exc_info=True)
            return redirect(request.url)

    return render_template("upload.html")

@upload_bp.route("/try_connection", methods=["POST"])
def try_connection():
    datastore = current_app.extensions["datastore"]

    success = datastore.try_internet_connection()
    return redirect(url_for("dashboard.index"))
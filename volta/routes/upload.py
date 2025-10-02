from flask import Blueprint, request, render_template, redirect, url_for, current_app
import os
import pandas as pd
from werkzeug.utils import secure_filename
import logging

upload_bp = Blueprint("upload", __name__, template_folder="../../templates")

UPLOAD_FOLDER = os.path.join(os.path.expanduser('~'), 'Downloads', 'volta_dashboard_data')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
ALLOWED_EXTENSIONS = {"csv", "xlsx"}

logger = logging.getLogger("volta.upload")

def allowed_file(filename):
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

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            file.save(filepath)

            try:
                if filename.endswith(".csv"):
                    df = pd.read_csv(filepath)
                else:
                    logger.warning("Unsupported file format")
                    return redirect(request.url)

                current_app.extensions["datastore"].set_df(df)

                parquet_path = os.path.join(
                    UPLOAD_FOLDER, os.path.splitext(filename)[0] + ".parquet"
                )
                df.to_parquet(parquet_path, index=False)

                current_app.config["DATA_PATH"] = parquet_path

                logger.info("File uploaded, loaded, and saved as parquet successfully!")
                return redirect(url_for("dashboard.index"))

            except Exception as e:
                logger.error(f"Error processing file: {e}", exc_info=True)
                return redirect(request.url)

    return render_template("upload.html")
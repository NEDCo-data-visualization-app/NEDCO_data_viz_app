"""CSV upload and remote-refresh endpoints."""

from __future__ import annotations

import hmac
import logging
from pathlib import Path

from flask import Blueprint, abort, current_app, flash, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

upload_bp = Blueprint("upload", __name__)

ALLOWED_EXTENSIONS = {"csv"}
logger = logging.getLogger("volta.upload")


def _uploads_dir() -> Path:
    uploads_dir = Path(current_app.config["UPLOADS_DIR"]).expanduser()
    uploads_dir.mkdir(parents=True, exist_ok=True)
    return uploads_dir


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _require_admin_token() -> None:
    """Reject writes without the configured ADMIN_TOKEN (no-op when none is configured)."""
    expected = current_app.config.get("ADMIN_TOKEN")
    if not expected:
        return
    supplied = request.form.get("token") or request.headers.get("X-Admin-Token") or ""
    if not hmac.compare_digest(str(supplied), str(expected)):
        logger.warning("Rejected %s without a valid admin token", request.path)
        abort(403)


@upload_bp.route("/upload", methods=["GET", "POST"])
def upload_file():
    if request.method == "GET":
        return render_template("upload.html")

    _require_admin_token()
    file = request.files.get("file")
    if file is None or not file.filename:
        flash("Please choose a CSV file to upload.", "warning")
        return redirect(request.url)
    if not allowed_file(file.filename):
        flash("Only .csv files are supported.", "warning")
        return redirect(request.url)

    filepath = _uploads_dir() / secure_filename(file.filename)
    try:
        file.save(str(filepath))
        added = current_app.extensions["datastore"].ingest_csv(filepath)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(request.url)
    except Exception:  # noqa: BLE001
        logger.exception("Error processing upload %s", filepath)
        flash("The file could not be loaded. Check that its columns match the dataset.", "danger")
        return redirect(request.url)
    finally:
        filepath.unlink(missing_ok=True)

    flash(f"Upload complete: {added:,} new rows added.", "success")
    return redirect(url_for("dashboard.index"))


@upload_bp.route("/try_connection", methods=["POST"])
def try_connection():
    _require_admin_token()
    ok, message = current_app.extensions["datastore"].try_internet_connection()
    flash(message, "success" if ok else "warning")
    return redirect(url_for("dashboard.index"))

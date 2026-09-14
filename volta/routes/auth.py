"""Optional shared passwords.

When VIEWER_PASSWORD and/or PRIVATE_PASSWORD are configured every page and
API call requires a signed-in session; the login page, static files and
/health stay open. The viewer password shows the dashboard in the mode the
deployment runs in (public mode hides meter and customer identifiers); the
private password additionally unlocks the private view for that session.
"""

from __future__ import annotations

import hmac
import time

from flask import Blueprint, current_app, jsonify, redirect, render_template, request, session, url_for

auth_bp = Blueprint("auth", __name__)

SESSION_KEY = "viewer_role"  # "viewer" or "private"
OPEN_ENDPOINTS = {"auth.login", "auth.logout", "static", "dashboard.health"}
API_PREFIXES = ("/predictions/api", "/filters/", "/options/", "/chart-data", "/pie-data", "/bar-data")


def login_required_enabled() -> bool:
    return bool(current_app.config.get("VIEWER_PASSWORD") or current_app.config.get("PRIVATE_PASSWORD"))


def is_authenticated() -> bool:
    return session.get(SESSION_KEY) in ("viewer", "private")


def has_private_access() -> bool:
    return session.get(SESSION_KEY) == "private"


def effective_public_mode() -> bool:
    """PUBLIC_MODE from config, unless this session signed in with the private password."""
    if has_private_access():
        return False
    return bool(current_app.config.get("PUBLIC_MODE", False))


def _role_for(password: str):
    for role, key in (("private", "PRIVATE_PASSWORD"), ("viewer", "VIEWER_PASSWORD")):
        expected = current_app.config.get(key)
        if expected and hmac.compare_digest(password.encode(), str(expected).encode()):
            return role
    return None


def _safe_next(default: str) -> str:
    target = request.values.get("next") or ""
    if target.startswith("/") and not target.startswith("//"):
        return target
    return default


def require_viewer_login():
    """before_request hook: gate everything behind the viewer password when one is set."""
    if not login_required_enabled():
        return None
    if request.endpoint is None or request.endpoint in OPEN_ENDPOINTS:
        return None
    if is_authenticated():
        return None
    wants_json = request.path.startswith(API_PREFIXES) or request.is_json
    if wants_json:
        return jsonify({"ok": False, "error": "Login required"}), 401
    if request.method != "GET":
        return redirect(url_for("auth.login"))
    return redirect(url_for("auth.login", next=request.full_path.rstrip("?")))


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if not login_required_enabled():
        return redirect(url_for("dashboard.index"))
    if is_authenticated():
        return redirect(_safe_next(url_for("dashboard.index")))

    error = None
    if request.method == "POST":
        role = _role_for(request.form.get("password") or "")
        if role:
            session.clear()
            session[SESSION_KEY] = role
            session.permanent = True
            return redirect(_safe_next(url_for("dashboard.index")))
        time.sleep(1)  # slow down password guessing
        error = "Incorrect password."

    return render_template("login.html", error=error, next=request.values.get("next", "")), (401 if error else 200)


@auth_bp.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("auth.login") if login_required_enabled() else url_for("dashboard.index"))

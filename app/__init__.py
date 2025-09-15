from flask import Flask
from .config import DevConfig
from .blueprints.main import bp as main_bp
from pathlib import Path

def create_app(config_object=DevConfig) -> Flask:
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(config_object)
    # Optional local overrides: instance/settings.py (not tracked)
    try:
        app.config.from_pyfile("settings.py")
    except FileNotFoundError:
        pass
    # Ensure data dir exists if you rely on it
    Path(app.config.get("DATA_DIR", ".")).mkdir(parents=True, exist_ok=True)
    app.register_blueprint(main_bp)
    return app

"""Local entry point: boots the Flask app and opens a browser tab."""

import os
import webbrowser
from threading import Timer

from volta.app import create_app

HOST = os.getenv("VOLTA_HOST", "127.0.0.1")
PORT = int(os.getenv("VOLTA_PORT", "5050"))

app = create_app()


def open_browser():
    webbrowser.open_new(f"http://{HOST}:{PORT}")


if __name__ == "__main__":
    Timer(1, open_browser).start()
    app.run(host=HOST, port=PORT, use_reloader=False, debug=False)

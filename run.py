"""Development entry point for running the Volta dashboard."""

import os
import sys
import webbrowser
from threading import Timer
from volta.app import create_app
from dotenv import load_dotenv

if getattr(sys, "frozen", False):
    load_dotenv(os.path.join(sys._MEIPASS, ".env"))
else:
    load_dotenv()  

app = create_app()

def open_browser():
    webbrowser.open_new("http://127.0.0.1:5000")

if __name__ == "__main__":
    Timer(1, open_browser).start()
    app.run(host="127.0.0.1", port=5000)

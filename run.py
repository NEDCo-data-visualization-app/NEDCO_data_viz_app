"""Development entry point for running the Volta dashboard."""

from volta import create_app

app = create_app()


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
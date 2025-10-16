# VoltaV Data Visualization Dashboard

VoltaV is a Flask-based analytics dashboard for the Northern Electricity Distribution Company (NEDCO). The app centralizes CSV or parquet exports into a DuckDB warehouse and exposes interactive tools to explore consumption, revenue, and metering KPIs. Filters, responsive charts, summary statistics, and CSV downloads are all available out of the box, and analysts can refresh the dataset by uploading new files directly from the UI.

## Table of contents
- [Key features](#key-features)
- [Project structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Configuration](#configuration)
- [Running the app](#running-the-app)
- [Working with data](#working-with-data)
- [Dashboard walkthrough](#dashboard-walkthrough)
- [Development tips](#development-tips)

## Key features
- **End-to-end data pipeline** – Loads CSV files into a local DuckDB database and can fall back to downloading a parquet export from Supabase when configured.
- **Upload workflow** – Analysts can upload fresh CSVs from the dashboard; files are validated, ingested into the DuckDB table, and then removed from disk.
- **Powerful filtering** – Date range pickers and dynamic checkbox filters (including asynchronous meter ID lookups) let users slice data by any categorical column while hiding low-level fields by default.
- **Interactive visualizations** – Time-series, composition, and city-level charts, plus descriptive statistics and tabular previews with export buttons, provide a complete analytical view.
- **Configurable metrics** – Centralized metric labels and frequency rules allow teams to rename or add KPIs without touching presentation logic.

## Project structure
```
NEDCO_data_viz_app/
├── run.py               # Local entry point that boots the Flask app
├── volta/
│   ├── app.py           # Flask application factory
│   ├── config.py        # Default settings and environment bindings
│   ├── routes/          # Dashboard & upload blueprints
│   ├── services/        # Data access and metrics helpers
│   ├── static/          # Compiled JS/CSS assets
│   └── templates/       # Jinja templates for the UI
├── models/              # Reserved for ML/forecasting artifacts
├── notebooks/           # Exploratory analysis (not served by Flask)
├── requirements*.txt    # Dependency pins for the dashboard & forecasting
└── README.md
```

## Prerequisites
- Python 3.9 or newer
- `pip` and `virtualenv` (or another environment manager)
- Optional: DuckDB CLI for inspecting the warehouse (`duckdb data/warehouse.duckdb`)

## Setup
1. **Clone the repository**
   ```bash
   git clone <repo-url>
   cd NEDCO_data_viz_app
   ```
2. **Create and activate a virtual environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Configuration
Create a `.env` file in the project root. At minimum, you can leave the variables blank to work entirely with local CSVs:
```env
# Remote parquet download (optional)
BUCKET_URL=""
SUPABASE_KEY=""

# DuckDB configuration (paths are relative to the repo root by default)
VOLTA_DUCKDB_PATH="data/warehouse.duckdb"
VOLTA_CSV_GLOB="data/uploads/*.csv"

# Dataset column overrides (only set these if your columns differ)
VOLTA_DATE_COL="chargedate"
VOLTA_DATE_FMT="%d-%b-%y"
```
Any value present in `.env` overrides the defaults defined in `volta/config.py`.

## Running the app
Start the development server with:
```bash
python run.py
```
The command loads environment variables, boots the Flask app, and opens your browser to <http://127.0.0.1:5000>.

## Working with data
1. **Initial load** – On first run, the app looks for CSV files that match `VOLTA_CSV_GLOB`. If it finds data, it builds the `prod.sales` table inside DuckDB; otherwise, it attempts to download a parquet file from `BUCKET_URL` using the provided Supabase API key.
2. **Persisted warehouse** – DuckDB lives on disk (`data/warehouse.duckdb` by default) so rebuilds are only triggered when the table is missing or you call `rebuild_from_csv()` manually.
3. **Uploading new CSVs** – Visit `/upload`, choose a CSV file, and submit. Valid files are ingested into the existing dataset, stored in DuckDB, and the temporary upload is deleted. The dashboard refreshes automatically once the new data is loaded.
4. **Schema expectations** – The dashboard assumes numeric columns for each configured metric (`kwh`, `paymoney`, `ghc` by default) and uses `chargedate` for date filtering. Update `volta/config.py` if your dataset uses different column names.

## Dashboard walkthrough
- **Filters panel** – Stickied on the left, providing date pickers, accordion-based categorical filters, a meter ID search, and quick-reset controls.
- **Charts** – A metric selector drives a time-series line chart, donut compositions, and totals by city. Each visualization offers a download button for offline reporting.
- **Summary stats** – Displays dataset coverage (date range, meters, locations, row/column counts) and per-metric aggregates (sum, mean, median, min, max).
- **Data table** – Renders a paginated preview of filtered records with an option to export the current result set as CSV.
- **Connectivity tools** – Buttons at the bottom of the filters card let operators test internet connectivity and ingest fresh data without leaving the app.

## Development tips
- Use the application factory (`volta.app.create_app`) to create testing instances with custom configuration mappings.
- Metrics and filter behavior are centralized in `volta/config.py`; update the `METRICS`, `FREQ_RULE`, or `EXCLUDE_COLS` dictionaries to adapt the UI to new datasets.
- Static assets are bundled under `volta/static`; run `npm` tooling there if you need to recompile JS/CSS (not required for basic development).
- For data debugging, open the DuckDB file with the CLI or Python REPL to validate tables and schemas before exposing them in the dashboard.

---
Questions or contributions are welcome—open an issue or submit a pull request when you extend the dashboard.

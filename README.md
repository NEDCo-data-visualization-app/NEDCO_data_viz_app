# VoltaV Data Visualization Dashboard

VoltaV is a Flask-based analytics dashboard for the Northern Electricity Distribution Company (NEDCO). The app keeps the prepaid sales dataset in a local DuckDB warehouse and exposes interactive tools to explore consumption, revenue, and metering KPIs. Filters, responsive charts, summary statistics, and CSV downloads are all available out of the box, and analysts can extend the dataset by uploading new CSV files directly from the UI. All frontend assets are bundled with the app, so it runs fully offline.

## Table of contents
- [Key features](#key-features)
- [Project structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Configuration](#configuration)
- [Running the app](#running-the-app)
- [Working with data](#working-with-data)
- [Dashboard walkthrough](#dashboard-walkthrough)
- [Forecasting & predictions](#forecasting--predictions)
- [Development tips](#development-tips)

## Key features
- **Local DuckDB warehouse** – The dataset lives in a single DuckDB file; every chart, statistic and export is a SQL query, so nothing is held in memory.
- **Upload workflow** – Analysts can upload CSVs from the dashboard; columns are matched by name, dates are parsed, duplicate rows are skipped, and the temporary file is removed afterwards. If no dataset exists yet the first upload creates it.
- **Optional remote refresh** – When `BUCKET_URL` is configured, **Try Internet Connection** downloads a parquet export and replaces the dataset.
- **Works offline** – Bootstrap and Chart.js are served from `volta/static/vendor`, so no internet connection is needed to use the dashboard.
- **Powerful filtering** – Date range pickers and dynamic checkbox filters (including asynchronous meter ID lookups) let users slice data by any categorical column while hiding low-level fields by default.
- **Interactive visualizations** – Time-series, composition, and city-level charts, plus descriptive statistics and tabular previews with export buttons, provide a complete analytical view.
- **Forecast explorer** – A dedicated predictions workspace lets analysts run LightGBM forecasts per meter or across the fleet, review charted projections, and export CSV previews for downstream analysis.
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
│   ├── static/          # JS modules, CSS and vendored libraries
│   └── templates/       # Jinja templates for the UI
├── models/              # LightGBM pickles (3 targets × 12 horizons)
├── notebooks/           # Exploratory analysis (not served by Flask)
├── build_*.py, prepare_warehouse_new.py  # One-off scripts that build the warehouse from Excel exports
├── requirements*.txt    # Dependencies for the dashboard & notebooks
└── README.md
```

## Prerequisites
- Python 3.9 or newer
- `pip` and `virtualenv` (or another environment manager)
- Optional: DuckDB CLI for inspecting the warehouse (`duckdb data/warehouse_new.duckdb`)

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
Configuration is read from environment variables, or from a `.env` file in the project root (or next to the packaged executable). Everything has a default, so an empty `.env` works:
```env
# DuckDB file holding the dataset (relative paths resolve from the project root / executable folder)
DB_PATH="data/warehouse_new.duckdb"
# Name of the table inside that file (legacy variable name)
PARQUET_PATH="merged_sales_customers_clean"

# Hide meter identifiers from the UI
PUBLIC_MODE="false"

# Optional remote parquet export used by "Try Internet Connection"
BUCKET_URL=""
SUPABASE_KEY=""

# Where uploaded CSVs are staged before ingestion (deleted afterwards)
UPLOADS_DIR="~/Downloads/volta/uploads"

# Dataset column overrides
VOLTA_DATE_COL="od_date"
VOLTA_DATE_FMT="%d-%b-%y"   # accepted in uploads in addition to ISO dates

# Forecast page: cut-off between history and forecast
FORECAST_AS_OF="2020-09-01"
MODEL_DIR="models"

# Server
VOLTA_HOST="127.0.0.1"
VOLTA_PORT="5050"
```
See `volta/config.py` for the full list.

## Running the app
Start the development server with:
```bash
python run.py
```
The command loads environment variables, boots the Flask app, and opens your browser to <http://127.0.0.1:5050>. If no dataset is present yet you land on the upload page.

### Running packaged builds
- **Windows (.exe)** – Download the latest executable, double-click to launch, and if Windows Defender SmartScreen displays a warning select **More info** and then **Run anyway** to proceed.
- **macOS (.app)** – After downloading the app bundle, Control-click the file in Finder, choose **Open**, and confirm the prompt so Gatekeeper trusts the unsigned build.

## Working with data
1. **Building the warehouse from the raw Excel exports** – Place the source workbooks in `data/` and run `python prepare_warehouse_new.py`. It calls `build_warehouse_new.py` and `build_links.py` as needed and produces the `merged_sales_customers_clean` table with these columns: `meterid`, `customer_no`, `od_date` (DATE), `ocd_paymoney`, `ocd_energy`, `ocd_cash_received`, `utility`, `tariff_type`.
2. **Persisted warehouse** – DuckDB lives on disk (`DB_PATH`), so the app starts instantly and nothing is rebuilt at startup.
3. **Uploading CSVs** – Use the **Update Data** card on the dashboard (or `/upload`). The CSV must contain the date column; other columns are matched by name (case-insensitive) and cast to the table's types. Rows that already exist are skipped. With no dataset present, the upload creates the table.
4. **Remote refresh** – With `BUCKET_URL` set, **Try Internet Connection** downloads that parquet file and replaces the dataset. Without it the button only reports whether the machine is online.
5. **Schema expectations** – The dashboard assumes numeric columns for each configured metric (`ocd_energy`, `ocd_paymoney`, `ocd_cash_received` by default) and uses `od_date` for date filtering. Update `volta/config.py` if your dataset uses different column names.

## Dashboard walkthrough
- **Filters panel** – Stickied on the left, providing date pickers, accordion-based categorical filters, a meter ID search, and quick-reset controls.
- **Charts** – A metric selector drives a time-series line chart, donut compositions, and totals by city. Each visualization offers a download button for offline reporting.
- **Summary stats** – Displays dataset coverage (date range, meters, locations, row/column counts) and per-metric aggregates (sum, mean, median, min, max).
- **Data table** – Renders a paginated preview of filtered records with an option to export the current result set as CSV.
- **Update Data** – Buttons below the filters card let operators refresh from the remote source or upload a CSV without leaving the app.

## Forecasting & predictions
- **Accessing the view** – Click **See Predictions** on the main dashboard. The private view (`/predictions/private`) adds a meter search; the public view (`/predictions`) only filters by location.
- **Where the numbers come from** – The page reads the `predict_all_cache` table in the DuckDB file. Charts show monthly history up to `FORECAST_AS_OF` (solid line) and the cached forecast after it (dashed line); the table preview and the CSV download come from the same table.
- **Populating the cache** – `POST /predictions/api/predict-all-cache` runs the 36 LightGBM models for every meter (using each meter's latest 24 months of history), replaces the cache, and returns the same payload as *Predict All*. It needs the `models/` folder and can take a while on the full dataset; it is not wired to a button yet.

## Forecast model
The models are trained in `notebooks/model_training.ipynb` and loaded by `PredictorLGBM` (`volta/services/predictor.py`):

- **36 models** – one LightGBM regressor per target (`ocd_paymoney`, `ocd_energy`, `ocd_cash_received`) and horizon (1–12 months), stored as `models/lgbm_<target>_h<h>.pkl`. They are scikit-learn wrappers, so `scikit-learn` must be installed.
- **41 features** – calendar and seasonal encodings, lags (1, 2, 3, 6, 12 months), rolling means (3, 6, 12 months), first differences, ratios and tariff interactions, all rebuilt inside DuckDB from the latest row per meter.
- **Direct multi-horizon forecasting** – each horizon has its own model; there is no recursion.

## Development tips
- Use the application factory (`volta.app.create_app`) to create testing instances with custom configuration mappings.
- Metrics and filter behavior are centralized in `volta/config.py`; update the `METRICS`, `FREQ_RULE`, or `EXCLUDE_COLS` dictionaries to adapt the UI to new datasets.
- Frontend code is plain ES modules under `volta/static/js` (no build step). Third-party libraries live in `volta/static/vendor`; update them by copying the `dist` files from the npm packages.
- For data debugging, open the DuckDB file with the CLI or Python REPL to validate tables and schemas before exposing them in the dashboard.

---
Questions or contributions are welcome—open an issue or submit a pull request when you extend the dashboard.

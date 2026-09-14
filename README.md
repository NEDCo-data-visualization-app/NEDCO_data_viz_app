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
- [Deploying to Render](#deploying-to-render)
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

# When set, uploads and remote refreshes require this token (set it on any
# deployment other people can reach)
ADMIN_TOKEN=""

# Shared passwords for viewing (leave empty for no login, e.g. the desktop app).
# VIEWER_PASSWORD shows the dashboard in the deployment's mode; PRIVATE_PASSWORD
# additionally unlocks the private view (identifiers visible) for that session.
VIEWER_PASSWORD=""
PRIVATE_PASSWORD=""
SESSION_COOKIE_SECURE="false"   # true behind HTTPS

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

## Deploying to Render
The repo contains a [Render Blueprint](render.yaml) that runs the dashboard in **public mode** from the `Dockerfile`, with the DuckDB warehouse on a persistent disk.

1. In Render choose **New → Blueprint**, connect this GitHub repository and pick the branch to deploy. Render reads `render.yaml` and creates a `voltav-dashboard` web service (Starter plan; persistent disks are not available on the free plan).
2. Render asks for two values before creating the service: `VIEWER_PASSWORD` (what colleagues type to see the public view) and `PRIVATE_PASSWORD` (unlocks meter and customer identifiers). Choose them like any shared password; both can be changed later in the **Environment** tab.
3. Wait for the first deploy, then open the service URL and sign in. With no data yet you land on the upload page.
4. Copy `ADMIN_TOKEN` from the service's **Environment** tab. It is generated automatically and is required for anything that changes the dataset.
5. Load the data, either by uploading a CSV export on the upload page (paste the token in the *Admin token* field), or by setting `BUCKET_URL` / `SUPABASE_KEY` in the Environment tab and pressing **Try Internet Connection**. The dataset persists on the disk across deploys.

Notes:
- The image excludes `models/` and `notebooks/` to stay small, so the prediction cache cannot be generated on Render; the predictions page shows history only unless a pre-populated warehouse is loaded.
- In public mode the dashboard hides meter and customer identifiers and the *Update Data* card; the upload page at `/upload` stays available for operators with the token. Signing in with `PRIVATE_PASSWORD` switches that session to the private view.
- Sessions last 30 days; **Sign out** at the top of every page ends one early.
- The service runs a single Gunicorn worker because DuckDB permits one writer per file.

## Dashboard walkthrough
- **Filter bar** – Date range, District and Account type (and a meter-number search in the private view) sit in one row above the content. *Quick periods* (last month, last 3 / 12 / 24 months, all data) are relative to the newest data and apply immediately.
- **Key figures** – Manager tiles computed over the current filters, each with the change versus the preceding period of equal length (whole-month ranges compare with the same months one period earlier; the comparison is omitted when there is no earlier data):
  - *Energy sold* – sum of `ocd_energy` (kWh), with the number of prepaid purchases.
  - *Amount paid* – sum of `ocd_paymoney` (GH₵). This is the revenue measure for prepaid customers; cash received is kept in the data for postpaid billing later but is not shown as a tile.
  - *Active customers* – distinct meters with at least one purchase in the period, with the number of districts.
  - *Spend per customer* – amount paid divided by customer-months (months in which a customer bought at least once), i.e. the average monthly spend of an active customer.
  - *Average price* – amount paid divided by energy sold (GH₵/kWh), a realised-tariff check.
  - *Residential* – share of active customers on a residential account type.
- **Charts** – One metric at a time (energy, amount paid, cash received): total over time, by district, and share by account type. Each chart downloads as a PNG on a white background.
- **Summary statistics** – Per-transaction sum, mean, median, min and max for each metric.
- **Data table** – A preview of the most recent filtered records, with export of the full filtered result as CSV.
- **Update data** – In the private view, the navigation links to the upload page (CSV append or refresh from the remote source).

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

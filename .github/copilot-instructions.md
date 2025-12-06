# AI Coding Instructions for NEDCO Data Visualization App

## Project Overview
**VoltaV** is a Flask-based analytics dashboard for electricity distribution data (NEDCO). It ingests CSVs into a **DuckDB warehouse**, exposes interactive visualizations, and provides LightGBM-based forecasting. The app supports file uploads, real-time filtering, and metric aggregations.

## Architecture

### Core Components
- **`volta/app.py`** – Flask application factory; creates app instance with DuckDB/DataFrame caching
- **`volta/config.py`** – Centralized configuration (metrics, column names, paths, exclusions)
- **`volta/services/datastore.py`** – DuckDB connection layer; manages `merged_sales_customers_clean` table
- **`volta/services/metrics.py`** – Metric mapping (label lookups, validation)
- **`volta/services/predictor.py`** – LightGBM model inference; recursive forecasting (fleet & per-meter)
- **`volta/routes/dashboard/`** – Blueprint with views (index, aggregates, charts, filters, downloads)
- **`volta/routes/upload.py`** – CSV upload handler; validates, ingests via DataStore, deletes temp file

### Data Flow
1. **Load** → DataStore checks local DuckDB; falls back to remote Parquet (Supabase) if configured
2. **Ingest** → CSV files matched by `CSV_GLOB` are auto-loaded on startup or via upload endpoint
3. **Query** → Flask routes execute SQL via DuckDB; results passed to templates as JSON or DataFrames
4. **Render** → Jinja templates + JavaScript (in `volta/static/js/`) build interactive UI

### Data Schema
**Primary table:** `merged_sales_customers_clean` (materialized in DuckDB)
- **Key columns:** `od_date` (DATE), `meterid`, `utility`, `tariff_type`, `od_cash_received`, `ocd_energy`, `ocd_paymoney`
- **Configured via:** `Config.EXCLUDE_COLS` (hidden from UI), `Config.METRICS` (metric labels), `Config.DATE_COL` (date column name)

## Critical Patterns

### Configuration-Driven UI
Metrics and filters are **not hardcoded**. Modify `volta/config.py`:
```python
METRICS = {
    "ocd_energy": "Energy (kWh)",          # key → label
    "ocd_paymoney": "Paymoney",
    "ocd_cash_received": "Cash Received (GHC)",
}
EXCLUDE_COLS = {"od_date", "month", ...}  # hidden from UI filters
DATE_COL = "od_date"                       # configurable date column
```
To add a metric or rename a column, update these dicts—no template changes needed.

### DuckDB as Single Source of Truth
- **No in-memory copies** – all filtering queries use DuckDB SQL (`run_query()`)
- **Lazy loading** – DataStore caches `merged_sales_customers_clean` on first access; rebuilds only when table is missing
- **Typed dates** – dates are cast to DATE type in DuckDB; string formats configured via `DATE_FMT` (default `"%d-%b-%y"`)

### Filter Parameter Serialization
Filters (date range, categorical selections, metric, frequency) are stored in a **`FilterParams`** object (`volta/utils/filter_params.py`). It converts UI selections to SQL WHERE clauses:
```python
params = FilterParams(start=date1, end=date2, selections={"utility": ["A", "B"]}, freq="M", metric="ocd_energy")
clause, sql_params = params.to_sql_where(date_col="od_date", available_columns=df.columns)
```
This pattern centralizes WHERE logic across all endpoints.

### Extension Registry Pattern
Flask extensions are stored in `app.extensions`:
```python
app.extensions["metrics"] = Metrics(app.config["METRICS"])
app.extensions["datastore"] = DataStore(app.config, metrics)
app.extensions["predictor"] = PredictorLGBM(...)  # lazy-initialized
```
Access in routes via: `get_metrics()`, `get_datastore()`, `get_predictor()` (defined in `volta/routes/dashboard/__init__.py`).

### Prediction (LightGBM) Workflow
1. **Fleet forecast** – `predict_all()` uses September 2020 cutoff; returns aggregated preview (top 10 rows) + full CSV export
2. **Single meter** – `predict_recursive_one()` resolves meter's latest observation, runs recursive inference, returns meter-specific export
3. **Features** – 47 engineered features (monthly seasonality, lags, rolling means, location/residential encodings); rebuilt per horizon step
4. **Recursive step** – append predicted row to working snapshot, recompute features for next month

## Developer Workflows

### Starting the App
```bash
source .venv/bin/activate
python run.py
```
- Loads `.env` file (optional Supabase config)
- Boots Flask on `127.0.0.1:5000`
- Opens browser automatically

### Rebuilding DuckDB from CSVs
```python
from volta.app import create_app
app = create_app()
with app.app_context():
    datastore = app.extensions["datastore"]
    datastore.rebuild_from_csv()  # drops & recreates merged_sales_customers_clean
```
Or via Flask shell: `flask shell` → `get_datastore().rebuild_from_csv()`

### Testing with Custom Config
```python
from volta.app import create_app
config = {
    "DUCKDB_PATH": "test.duckdb",
    "METRICS": {"col_a": "Label A"},
    "DATE_COL": "date_column",
}
app = create_app(config_object=config)
```

### Inspecting DuckDB
```bash
duckdb data/warehouse_new.duckdb
> SELECT COUNT(*) FROM merged_sales_customers_clean;
> DESCRIBE merged_sales_customers_clean;
```

## Project-Specific Conventions

### Naming
- **Routes** → snake_case, plural when returning lists: `/filters/options`, `/charts/timeseries`
- **Templates** → underscore prefix for partials: `_filters.html`, `_charts.html`
- **Python modules** → lowercase, descriptive: `datastore.py`, `predictor.py`, `helpers.py`

### Return Types
- **JSON endpoints** → return `jsonify({...})` with consistent keys (`options`, `dates`, `rows`, `data`)
- **HTML endpoints** → use Macros in `volta/templates/macros/ui.html` for consistent UI components
- **CSV downloads** → served via `Response(..., mimetype="text/csv")`

### Error Handling
- Log via `logger.info()`, `logger.warning()`, `logger.error()` (initialized in `app.py`)
- Silent fallbacks: if remote Parquet fails, try local CSV; if no local data, return empty DataFrame
- Avoid raising exceptions in endpoints; catch and log instead

### Path Resolution
Use **absolute paths** when dealing with models, configs, or data files:
```python
from pathlib import Path
app_root = Path(current_app.root_path).resolve()
project_root = app_root.parent
bundle_path = project_root / "models" / "lgbm_bundle.pkl"
```
This ensures paths work in both dev (`python run.py`) and packaged builds (`.exe`, `.app`).

## External Dependencies & Integration Points
- **Flask** – web framework; extension-based service injection
- **DuckDB** – embedded SQL database; no server required
- **Pandas** – data wrangling, preprocessing
- **LightGBM** – time-series forecasting models (binary artifact in `models/`)
- **Supabase** – optional remote Parquet source (requires `BUCKET_URL` and `SUPABASE_KEY`)
- **Jinja2** – template rendering
- **Python 3.9+** – runtime requirement

## Key Files by Task

| Task | Key Files |
|------|-----------|
| Add a new metric | `volta/config.py` (METRICS dict), then update templates as needed |
| Change column names | `volta/config.py` (DATE_COL, EXCLUDE_COLS, metric keys), rebuild DuckDB |
| Add a filter | `volta/routes/dashboard/filters.py`, `volta/utils/filter_params.py` |
| Add a chart type | `volta/routes/dashboard/charts.py` (SQL), `volta/static/js/charts/` (JavaScript) |
| Debug data issues | Use DuckDB CLI or `datastore.run_query()` to inspect raw table |
| Add forecasting features | `volta/services/predictor.py` (feature engineering), retrain LightGBM, save to `models/lgbm_bundle.pkl` |
| Style UI components | `volta/static/css/app.css`, `volta/templates/macros/ui.html` |

## Debugging Tips
- **Check app logs** – Flask prints to stdout; look for `[volta]` prefix
- **Inspect config** – in routes, print `current_app.config` to verify loaded values
- **Validate SQL** – use DuckDB CLI to test queries before integrating into endpoints
- **Browser DevTools** → Network tab shows JSON payloads from `/filters/options`, `/charts/*` endpoints
- **Frozen app (.exe/.app)** – uses `sys._MEIPASS` for path resolution; test packaged builds with `pyinstaller`

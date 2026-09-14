"""End-to-end checks against a synthetic dataset (no real data required)."""

from __future__ import annotations

import datetime as dt
import io
import random
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from volta.app import create_app

ROOT = Path(__file__).resolve().parent.parent
TABLE = "merged_sales_customers_clean"


def _synthetic_rows(n_meters: int = 20, start=dt.date(2018, 1, 5), end=dt.date(2020, 9, 15)):
    rng = random.Random(1)
    utilities = ["Techiman", "Wenchi", "Akumadan", "Busunya"]
    rows = []
    for m in range(1, n_meters + 1):
        utility, tariff = rng.choice(utilities), rng.choice(["Residential", "Non Residential"])
        d = start
        while d <= end:
            rows.append(
                dict(
                    meterid=str(11010000000 + m), customer_no=str(501001000000 + m), od_date=d,
                    ocd_paymoney=round(rng.uniform(5, 200), 2), ocd_energy=round(rng.uniform(10, 400), 2),
                    ocd_cash_received=round(rng.uniform(5, 200), 2), utility=utility, tariff_type=tariff,
                )
            )
            d += dt.timedelta(days=rng.choice([14, 30, 31]))
    return rows


def _csv_bytes(rows, date_fmt="%Y-%m-%d") -> bytes:
    df = pd.DataFrame(rows)
    df["od_date"] = [d.strftime(date_fmt) for d in df["od_date"]]
    return df.to_csv(index=False).encode()


def _make_app(tmp_path, with_data=True, public=False):
    db = tmp_path / "warehouse.duckdb"
    if with_data:
        con = duckdb.connect(str(db))
        con.register("df", pd.DataFrame(_synthetic_rows()))
        con.execute(f"CREATE TABLE {TABLE} AS SELECT * EXCLUDE (od_date), od_date::DATE AS od_date FROM df")
        con.close()
    app = create_app(
        {"DB_PATH": str(db), "PARQUET_PATH": TABLE, "PUBLIC_MODE": public, "UPLOADS_DIR": str(tmp_path / "uploads"),
         "MODEL_DIR": str(ROOT / "models"), "TESTING": True}
    )
    return app


@pytest.fixture
def client(tmp_path):
    return _make_app(tmp_path).test_client()


@pytest.mark.parametrize(
    "method,url",
    [
        ("GET", "/"),
        ("GET", "/?start_date=2019-01-01&end_date=2019-12-31&utility=Techiman&metric=ocd_energy,ocd_paymoney&freq=M"),
        ("GET", "/health"),
        ("GET", "/chart-data?metric=ocd_energy,ocd_paymoney&freq=M"),
        ("GET", "/chart-data?metric=ocd_energy&freq=W&agg=sum"),
        ("GET", "/pie-data?metric=ocd_energy"),
        ("GET", "/bar-data?metric=ocd_energy,ocd_cash_received"),
        ("GET", "/download-csv?utility=Wenchi"),
        ("GET", "/options/meterid?q=1101&limit=5"),
        ("POST", "/options/meterid"),
        ("POST", "/filters/options"),
        ("GET", "/public-dashboard"),
        ("POST", "/reset-dashboard"),
        ("GET", "/predictions"),
        ("GET", "/predictions/private"),
        ("POST", "/predictions/api/predict-all"),
        ("GET", "/predictions/download"),
        ("GET", "/predictions/download?meterid=11010000001"),
        ("GET", "/upload"),
    ],
)
def test_routes_respond(client, method, url):
    resp = client.get(url) if method == "GET" else client.post(url, json={})
    assert resp.status_code == 200, resp.get_data(as_text=True)[:300]


def test_chart_and_aggregates_have_values(client):
    chart = client.get("/chart-data?metric=ocd_energy&freq=M").get_json()
    assert chart["labels"] and len(chart["values"]["ocd_energy"]) == len(chart["labels"])
    bars = client.get("/bar-data?metric=ocd_energy").get_json()
    assert bars and set(bars[0]["labels"]) == {"Techiman", "Wenchi", "Akumadan", "Busunya"}


def test_filter_options_narrow_with_selection(client):
    resp = client.post("/filters/options", json={"selections": {"utility": ["Techiman"]}}).get_json()
    assert resp["options"]["utility"] == ["Techiman"]
    assert resp["rows"] > 0 and resp["dates"]["min"]


def test_public_mode_hides_meter_ids(tmp_path):
    client = _make_app(tmp_path, public=True).test_client()
    assert b"Meter ID" not in client.get("/").data
    assert b"predictionMeterSearch" not in client.get("/predictions").data


def test_fresh_install_shows_upload_page_and_upload_bootstraps_table(tmp_path):
    client = _make_app(tmp_path, with_data=False).test_client()
    assert b"No data loaded" in client.get("/").data

    rows = _synthetic_rows(n_meters=5)
    resp = client.post("/upload", data={"file": (io.BytesIO(_csv_bytes(rows)), "b.csv")},
                       content_type="multipart/form-data", follow_redirects=True)
    assert resp.status_code == 200 and f"{len(rows):,} new rows added".encode() in resp.data
    assert client.get("/health").get_json()["rows"] == len(rows)

    # Re-upload is a no-op; legacy date format and shuffled/uppercase headers are accepted.
    resp = client.post("/upload", data={"file": (io.BytesIO(_csv_bytes(rows)), "b.csv")},
                       content_type="multipart/form-data", follow_redirects=True)
    assert b"0 new rows added" in resp.data
    extra = _synthetic_rows(n_meters=2, start=dt.date(2020, 10, 3), end=dt.date(2020, 12, 1))
    df = pd.read_csv(io.BytesIO(_csv_bytes(extra, "%d-%b-%y")))
    df = df[list(reversed(df.columns))]
    df.columns = [c.upper() for c in df.columns]
    resp = client.post("/upload", data={"file": (io.BytesIO(df.to_csv(index=False).encode()), "c.csv")},
                       content_type="multipart/form-data", follow_redirects=True)
    assert f"{len(extra)} new rows added".encode() in resp.data
    assert client.get("/health").get_json()["rows"] == len(rows) + len(extra)

    # Bad files are rejected with a message, never a 500, and nothing is left on disk.
    resp = client.post("/upload", data={"file": (io.BytesIO(b"a,b\n1,2\n"), "junk.csv")},
                       content_type="multipart/form-data", follow_redirects=True)
    assert resp.status_code == 200 and b"alert-danger" in resp.data
    assert not list((tmp_path / "uploads").iterdir())


def test_try_connection_never_crashes(client):
    resp = client.post("/try_connection", follow_redirects=True)
    assert resp.status_code == 200 and b"alert-" in resp.data


def test_predictions_read_from_cache(client):
    payload = client.post("/predictions/api/predict-all", json={"utility": ["Techiman"]}).get_json()
    assert payload["ok"] and payload["scope"] == "all" and payload["charts"]["historical"]
    assert client.post("/predictions/api/predict", json={}).status_code == 400
    body = client.get("/predictions/download").get_data(as_text=True)
    assert body.startswith("meterid,as_of,horizon")


@pytest.mark.skipif(not (ROOT / "models" / "lgbm_energy_h1.pkl").exists(), reason="model pickles not present")
def test_predict_all_cache_populates_and_serves(client):
    first = client.post("/predictions/api/predict-all-cache", json={}).get_json()
    assert first["ok"] and first["cached_rows"] == 20 * 12
    second = client.post("/predictions/api/predict-all-cache", json={}).get_json()
    assert second["cached_rows"] == 20 * 12, "re-running must replace, not duplicate"

    one = client.post("/predictions/api/predict-all", json={"meterid": "11010000001"}).get_json()
    assert one["scope"] == "meter" and one["row_count"] == 12 and len(one["charts"]["forecast"]) == 12
    body = client.get("/predictions/download?meterid=11010000001").get_data(as_text=True)
    assert body.count("\n") == 13


def test_admin_token_guards_write_endpoints(tmp_path):
    app = _make_app(tmp_path, public=True)
    app.config["ADMIN_TOKEN"] = "s3cret"
    client = app.test_client()

    # Public dashboard no longer shows the Update Data card; the upload page asks for the token.
    assert b"Update Data" not in client.get("/").data
    assert b"Admin token" in client.get("/upload").data

    csv = _csv_bytes(_synthetic_rows(n_meters=1))
    denied = client.post("/upload", data={"file": (io.BytesIO(csv), "x.csv")}, content_type="multipart/form-data")
    assert denied.status_code == 403
    assert client.post("/try_connection").status_code == 403
    assert client.post("/try_connection", data={"token": "wrong"}).status_code == 403

    allowed = client.post("/upload", data={"file": (io.BytesIO(csv), "x.csv"), "token": "s3cret"},
                          content_type="multipart/form-data", follow_redirects=True)
    assert allowed.status_code == 200 and b"new rows added" in allowed.data
    assert client.post("/try_connection", headers={"X-Admin-Token": "s3cret"}).status_code == 302


def test_gunicorn_entrypoint_exposes_app(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "w.duckdb"))
    import importlib
    import run
    importlib.reload(run)
    assert run.app.name == "volta.app"


def test_viewer_password_gates_everything_except_login_and_health(tmp_path):
    app = _make_app(tmp_path, public=True)
    app.config["VIEWER_PASSWORD"] = "letmein"
    client = app.test_client()

    # Pages redirect to the login page, API calls get a JSON 401, health stays open.
    resp = client.get("/?utility=Techiman")
    assert resp.status_code == 302 and resp.headers["Location"].startswith("/login?next=")
    assert client.get("/predictions").status_code == 302
    assert client.get("/chart-data?metric=ocd_energy").status_code == 401
    assert client.post("/predictions/api/predict-all", json={}).status_code == 401
    assert client.post("/filters/options", json={}).status_code == 401
    assert client.get("/download-csv").status_code == 302
    assert client.get("/health").status_code == 200
    assert client.get("/static/css/app.css").status_code == 200
    assert b"Sign in" in client.get("/login").data

    # Wrong password: stays out.
    resp = client.post("/login", data={"password": "nope"})
    assert resp.status_code == 401 and b"Incorrect password" in resp.data
    assert client.get("/").status_code == 302

    # Right password: redirected to the requested page, then everything works.
    resp = client.post("/login", data={"password": "letmein", "next": "/?utility=Techiman"})
    assert resp.status_code == 302 and resp.headers["Location"] == "/?utility=Techiman"
    page = client.get("/")
    assert page.status_code == 200 and b"Sign out" in page.data
    assert client.get("/chart-data?metric=ocd_energy").status_code == 200
    assert client.get("/login").status_code == 302  # already signed in

    # Open redirects are not followed.
    client.post("/logout")
    resp = client.post("/login", data={"password": "letmein", "next": "https://evil.example"})
    assert resp.headers["Location"] == "/"

    # Sign out locks it again.
    client.post("/logout")
    assert client.get("/").status_code == 302


def test_no_viewer_password_means_no_login(client):
    assert client.get("/").status_code == 200
    assert client.get("/login").status_code == 302  # nothing to log in to
    assert b"Sign out" not in client.get("/").data


def test_private_password_unlocks_private_view_on_a_public_deployment(tmp_path):
    app = _make_app(tmp_path, public=True)
    app.config["VIEWER_PASSWORD"] = "viewer-pw"
    app.config["PRIVATE_PASSWORD"] = "private-pw"
    client = app.test_client()

    # Viewer password: public view, identifiers hidden.
    client.post("/login", data={"password": "viewer-pw"})
    page = client.get("/")
    assert page.status_code == 200 and b"Public view" in page.data and b"Meter ID" not in page.data
    assert b"predictionMeterSearch" not in client.get("/predictions").data
    assert b"predictionMeterSearch" not in client.get("/predictions/private").data  # cannot be forced
    assert client.post("/predictions/api/predict-all", json={"meterid": "11010000001"}).get_json()["scope"] == "all"

    # Private password: same URLs, identifiers visible, meter search available.
    client.post("/logout")
    client.post("/login", data={"password": "private-pw"})
    page = client.get("/")
    assert page.status_code == 200 and b"Private view" in page.data and b"Meter ID" in page.data
    assert b"/predictions/private" in page.data  # "See Predictions" links to the private page
    assert b"predictionMeterSearch" in client.get("/predictions/private").data
    assert client.post("/predictions/api/predict-all", json={"meterid": "11010000001"}).get_json()["scope"] == "meter"
    assert b"Meter ID" not in client.get("/public-dashboard").data  # explicit public route stays public
    assert b"predictionMeterSearch" not in client.get("/predictions").data  # explicit public route stays public

    # Sign out drops the private access.
    client.post("/logout")
    assert client.get("/").status_code == 302


def test_upload_accepts_legacy_shiny_export(tmp_path):
    """The older wkfile_shiny.csv export uses different column names and dd-Mon-yy dates."""
    app = _make_app(tmp_path, with_data=False)
    client = app.test_client()
    csv = (
        "meterid,chargedate,chargedate_str,loc,res,month,month_str,kwh,year,ghc,paymoney\n"
        "1,15-Feb-19,15-Feb-2019,Techiman [13],N-Resid [0],Feb-19,01-Feb-2019,14.2,2019,23.1705,140\n"
        "1,11-Mar-19,11-Mar-2019,Techiman [13],N-Resid [0],Mar-19,01-Mar-2019,57.4,2019,38.8886,40\n"
        "2,20-Apr-19,20-Apr-2019,Wenchi [2],Resid [1],Apr-19,01-Apr-2019,57.4,2019,38.8886,20\n"
    ).encode()
    resp = client.post("/upload", data={"file": (io.BytesIO(csv), "wkfile_shiny.csv")},
                       content_type="multipart/form-data", follow_redirects=True)
    assert resp.status_code == 200 and b"3 new rows added" in resp.data, resp.data[:400]

    health = client.get("/health").get_json()
    assert health["rows"] == 3
    datastore = app.extensions["datastore"]
    cols = {r["column_name"]: r["column_type"] for r in datastore.run_query(f"DESCRIBE {datastore.table_sql}")}
    for canonical in ("od_date", "utility", "tariff_type", "ocd_energy", "ocd_cash_received", "ocd_paymoney"):
        assert canonical in cols, cols
    assert cols["od_date"] == "DATE" and "kwh" not in cols and "loc" not in cols

    chart = client.get("/chart-data?metric=ocd_energy&freq=M").get_json()
    assert chart["labels"] == ["2019-02", "2019-03", "2019-04"]
    bars = client.get("/bar-data?metric=ocd_paymoney").get_json()
    assert set(bars[0]["labels"]) == {"Techiman [13]", "Wenchi [2]"}
    assert b"Meter ID" in client.get("/").data


def test_branding_and_layout(client):
    page = client.get("/").get_data(as_text=True)
    assert 'img/nedco.png' in page and 'alt="NEDCo logo"' in page
    assert "Data through Sep 2020" in page  # synthetic data ends 2020-09-15
    assert page.count('class="btn-check metric-checkbox"') == 3  # one metric at a time
    assert 'id="lineChartTotal"' in page and 'id="barChart"' in page and 'id="pieChart"' in page
    assert 'id="lineChart"' not in page  # the per-transaction mean chart is gone
    assert "Customers" in page and "Transactions" in page  # KPI tiles
    assert "District" in page and "Account type" in page
    assert client.get("/static/img/favicon-32.png").status_code == 200
    assert client.get("/static/js/charts/theme.js").status_code == 200


def test_data_extent_updates_after_upload(tmp_path):
    client = _make_app(tmp_path, with_data=False).test_client()
    assert "Data through" not in client.get("/").get_data(as_text=True)
    rows = _synthetic_rows(n_meters=2, start=dt.date(2021, 3, 1), end=dt.date(2021, 5, 20))
    client.post("/upload", data={"file": (io.BytesIO(_csv_bytes(rows)), "b.csv")},
                content_type="multipart/form-data", follow_redirects=True)
    assert "Data through May 2021" in client.get("/").get_data(as_text=True)

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

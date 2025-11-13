#!/usr/bin/env python
import subprocess
from pathlib import Path

import duckdb
import pandas as pd


DB_PATH = Path("./data/warehouse_new.duckdb")
REQUIRED_WAREHOUSE_TABLES = [
    "sales_2012_2017",
    "sales_2018_2019",
    "customer_list_xls",
    "customer_list_xlsx",
]
MERGED_TABLE = "merged_sales_customers"
CLEAN_TABLE = "merged_sales_customers_clean"  # name for the final cleaned table


def run_script(script_name: str):
    """
    Run another Python script in the same directory as this script.
    Raises if it fails.
    """
    script_path = Path(__file__).resolve().parent / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")
    subprocess.run(
        ["python", str(script_path)],
        check=True,
    )


def connect_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    query = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = ?
    """
    res = con.execute(query, [table_name]).fetchone()
    return res is not None


def ensure_warehouse(con: duckdb.DuckDBPyConnection):
    """
    Ensure that warehouse_new.duckdb exists and has the four base tables.
    If the file or any table is missing, run build_warehouse_new.py.
    """
    need_rebuild = False

    if not DB_PATH.exists():
        need_rebuild = True
    else:
        for t in REQUIRED_WAREHOUSE_TABLES:
            if not table_exists(con, t):
                need_rebuild = True
                break

    if need_rebuild:
        print("Rebuilding warehouse_new with build_warehouse_new.py...")
        run_script("build_warehouse_new.py")
        con.close()
        # reopen after rebuild
        con = connect_db()

    return con


def ensure_links(con: duckdb.DuckDBPyConnection):
    """
    Ensure merged_sales_customers exists.
    If not, run build_links.py.
    """
    if not table_exists(con, MERGED_TABLE):
        print("Building merged_sales_customers with build_links.py...")
        run_script("build_links.py")
        con.close()
        con = connect_db()
        if not table_exists(con, MERGED_TABLE):
            raise RuntimeError(
                f"{MERGED_TABLE} still does not exist after running build_links.py"
            )
    return con


def clean_and_save(con: duckdb.DuckDBPyConnection):
    """
    - Load merged_sales_customers
    - Keep only selected columns
    - Strip time from od_date (keep only date)
    - Enforce sane dtypes (strings, floats, DATE)
    - Drop rows containing ANY NaN values
    - Save to a new DuckDB table
    """
    cols = [
        "meterid",
        "customer_no",
        "od_date",
        "ocd_energy",
        "ocd_cash_received",
        "utility",
        "tariff_type",
    ]

    # Load data
    query = f"SELECT {', '.join(cols)} FROM {MERGED_TABLE}"
    df = con.execute(query).df()

    # Convert od_date to date only
    if "od_date" in df.columns:
        df["od_date"] = pd.to_datetime(df["od_date"], errors="coerce").dt.date

    # Enforce types
    string_cols = ["meterid", "customer_no", "utility", "tariff_type"]
    for c in string_cols:
        if c in df.columns:
            df[c] = df[c].astype("string")

    float_cols = ["ocd_energy", "ocd_cash_received"]
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Drop any rows with missing data (instead of dropping columns)
    df = df.dropna(how="any")

    # Drop duplicates
    df = df.drop_duplicates()

    # Write cleaned table
    con.execute(f"DROP TABLE IF EXISTS {CLEAN_TABLE}")
    con.register("clean_df", df)
    con.execute(f"CREATE TABLE {CLEAN_TABLE} AS SELECT * FROM clean_df")
    con.unregister("clean_df")

    # Force od_date to proper DATE type
    if "od_date" in df.columns:
        con.execute(
            f"""
            ALTER TABLE {CLEAN_TABLE}
            ALTER COLUMN od_date
            SET DATA TYPE DATE
        """
        )

    print(f"Saved cleaned data to table '{CLEAN_TABLE}' in {DB_PATH}")



def main():
    # If DB doesn't exist at all, connect_db() will create an empty file;
    # ensure_warehouse will then trigger build_warehouse_new.py.
    con = connect_db()

    # Make sure base warehouse tables exist
    con = ensure_warehouse(con)

    # Make sure merged_sales_customers exists
    con = ensure_links(con)

    # Clean and save final table
    clean_and_save(con)

    con.close()


if __name__ == "__main__":
    main()

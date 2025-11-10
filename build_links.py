# build_links.py
import duckdb
from pathlib import Path

DB_PATH = Path("data") / "warehouse_new.duckdb"

# choose the meter column from a table by scanning its schema
def pick_meter_col(con, table):
    cols = con.execute(f"PRAGMA table_info('{table}')").fetchdf()["name"].str.lower().tolist()
    # priority list by common patterns in your files
    prefs = [
        "meter_no.", "meter_no", "meterid", "meter_id", "meter number", "meter-number",
        "meter_no_", "meter_no__", "meter"  # fallbacks
    ]
    # exact then contains
    for p in prefs:
        if p in cols:
            return p
    for c in cols:
        if "meter" in c:
            return c
    raise RuntimeError(f"No meter-like column found in {table}. Columns: {cols}")

def main():
    con = duckdb.connect(str(DB_PATH), read_only=False)

    # 0) sanity: required tables
    have = set(con.execute("SHOW TABLES").fetchdf()["name"])
    need = {"sales_2012_2017", "sales_2018_2019", "customer_list_xls", "customer_list_xlsx"}
    missing = need - have
    if missing:
        raise SystemExit(f"Missing tables: {sorted(missing)}")

    # 1) merge sales tables
    con.execute("""
        CREATE OR REPLACE TABLE electricity_sales_all AS
        SELECT *, '2012_2017' AS vintage FROM sales_2012_2017
        UNION ALL
        SELECT *, '2018_2019' AS vintage FROM sales_2018_2019;
    """)

    # 2) add normalized meter key to sales: digits only, take last 11
    #    keep od_date as TIMESTAMP
    con.execute("""
        CREATE OR REPLACE TABLE electricity_sales_all_norm AS
        SELECT
            *,
            RIGHT(regexp_replace(CAST(meterid AS VARCHAR), '[^0-9]', ''), 11) AS meterid_norm
        FROM electricity_sales_all;
    """)

    # 3) unify customers with dynamic meter column detection
    meter_xls  = pick_meter_col(con, "customer_list_xls")
    meter_xlsx = pick_meter_col(con, "customer_list_xlsx")

    con.execute(f"""
        CREATE OR REPLACE TABLE customers_union AS
        SELECT * FROM customer_list_xls
        UNION ALL
        SELECT * FROM customer_list_xlsx;
    """)

    # 4) normalize customer meter numbers: digits only, last 11; dedupe by key
    #    keep the first row per normalized key
    con.execute(f"""
        CREATE OR REPLACE TABLE customers_norm AS
        WITH base AS (
            SELECT
                *,
                RIGHT(regexp_replace(CAST("{meter_xls}" AS VARCHAR), '[^0-9]', ''), 11) AS meter_no_norm
            FROM customer_list_xls
            UNION ALL
            SELECT
                *,
                RIGHT(regexp_replace(CAST("{meter_xlsx}" AS VARCHAR), '[^0-9]', ''), 11) AS meter_no_norm
            FROM customer_list_xlsx
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY meter_no_norm ORDER BY meter_no_norm) AS rn
            FROM base
            WHERE meter_no_norm IS NOT NULL AND meter_no_norm <> ''
        )
        SELECT * EXCLUDE(rn)
        FROM ranked
        WHERE rn = 1;
    """)

    # 5) join sales ↔ customers on normalized meter key
    #    include all sales columns + selected customer columns to avoid name collisions
    #    first, get customer columns minus the meter columns to avoid duplicates
    cust_cols_df = con.execute("PRAGMA table_info('customers_norm')").fetchdf()
    cust_cols = [c for c in cust_cols_df["name"].tolist() if c.lower() not in {"meter_no_norm"}]

    # build SELECT list for customers with alias `c`
    cust_select_list = ", ".join([f"c.\"{c}\"" for c in cust_cols])

    con.execute(f"""
        CREATE OR REPLACE TABLE merged_sales_customers AS
        SELECT
            s.*,
            c.meter_no_norm AS customer_meter_no_norm,
            {cust_select_list}
        FROM electricity_sales_all_norm AS s
        INNER JOIN customers_norm AS c
            ON s.meterid_norm = c.meter_no_norm;
    """)

    # 6) QA summaries
    stats = con.execute("""
        WITH s AS (
            SELECT COUNT(DISTINCT meterid_norm) AS sales_keys FROM electricity_sales_all_norm
        ),
        c AS (
            SELECT COUNT(DISTINCT meter_no_norm) AS customer_keys FROM customers_norm
        ),
        m AS (
            SELECT COUNT(DISTINCT meterid_norm) AS matched_keys FROM merged_sales_customers
        )
        SELECT s.sales_keys, c.customer_keys, m.matched_keys
        FROM s, c, m;
    """).fetchdf()
    print(stats.to_string(index=False))

    # optional: date span and gaps of merged
    span = con.execute("""
        SELECT MIN(od_date) AS min_dt, MAX(od_date) AS max_dt, COUNT(*) AS rows
        FROM merged_sales_customers;
    """).fetchdf()
    print(span.to_string(index=False))

    con.close()
    print("Done: electricity_sales_all, electricity_sales_all_norm, customers_norm, merged_sales_customers")

if __name__ == "__main__":
    main()

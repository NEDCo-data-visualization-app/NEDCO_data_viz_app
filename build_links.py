# build_links.py
# Build only the final merged table using sales_2012_2017, sales_2018_2019,
# and customer_list_xls (cleaned: header-in-rows -> proper columns).
# Meter key rule: digits only -> pad to 14 digits, take last 14.

import re
from pathlib import Path
import duckdb
import pandas as pd

DB_PATH = Path("data") / "warehouse_new.duckdb"

# ---------------- helpers ----------------

KEYWORDS = ["meter", "customer", "account", "name", "tariff", "region", "area", "contact"]

def slug(s: str) -> str:
    s = re.sub(r"\s+", "_", s.strip().lower())
    s = re.sub(r"[^\w]+", "_", s).strip("_")
    return s or "col"

def find_header_row(df: pd.DataFrame) -> int:
    up = df.fillna("").astype(str)
    for i in range(min(len(up), 25)):
        row = " ".join(up.iloc[i].tolist()).lower()
        if any(k in row for k in KEYWORDS):
            return i
    return 0

def promote_header(df: pd.DataFrame) -> pd.DataFrame:
    hdr = find_header_row(df)
    header_vals = df.iloc[hdr].fillna("").astype(str).tolist()
    cols, seen = [], {}
    for i, v in enumerate(header_vals):
        name = slug(v) or f"col_{i}"
        seen[name] = seen.get(name, 0) + 1
        if seen[name] > 1:
            name = f"{name}_{seen[name]}"
        cols.append(name)
    out = df.iloc[hdr + 1:].copy().reset_index(drop=True)
    out.columns = cols
    out = out.dropna(how="all", axis=1)
    out = out.loc[:, ~out.columns.str.fullmatch(r"unnamed_\d+|col_\d+", case=False)]
    return out

def pick_meter_col_from_df(df: pd.DataFrame) -> str:
    for c in df.columns:
        lc = c.lower()
        if "meter" in lc and ("no" in lc or "number" in lc):
            return c
    for c in df.columns:
        if "meter" in c.lower():
            return c
    raise RuntimeError(f"No meter-like column in cleaned customer DF. Columns: {df.columns.tolist()}")

# ✔ NEW normalization rule
def norm14(series: pd.Series) -> pd.Series:
    return (series.astype(str)
                  .str.replace(r"\D", "", regex=True)
                  .str.zfill(14)          
                  .str[-14:]              
                  .fillna(""))

def find_table(con, want_exact: str, fallbacks: list[str]) -> str:
    names = con.sql("SHOW TABLES").fetchdf()["name"].str.lower().tolist()
    if want_exact in names:
        return want_exact
    for f in fallbacks:
        if f in names:
            return f
    all_names = con.sql("SELECT name FROM duckdb_tables()").fetchdf()["name"].str.lower().tolist()
    if want_exact.startswith("sales_2012"):
        c = [n for n in all_names if "sales" in n and "2012" in n]
        return c[0] if c else ""
    if want_exact.startswith("sales_2018"):
        c = [n for n in all_names if "sales" in n and ("2018" in n or "2019" in n)]
        return c[0] if c else ""
    if want_exact.startswith("customer_list_xls"):
        c = [n for n in all_names if "customer" in n and "list" in n and "xls" in n]
        return c[0] if c else ""
    return ""

# ---------------- main ----------------

def main():
    con = duckdb.connect(str(DB_PATH), read_only=False)

    # discover required input tables (prefer xls for customers)
    t_sales_a = find_table(con, "sales_2012_2017", ["electricity_sales_2012_2017"])
    t_sales_b = find_table(con, "sales_2018_2019", ["electricity_sales_2018_2019"])
    t_cust    = find_table(con, "customer_list_xls", ["customer_list"])

    missing = [n for n in [t_sales_a, t_sales_b, t_cust] if not n]
    if missing:
        print("Tables present:\n", con.sql("SHOW TABLES").fetchdf())
        raise SystemExit("Required tables not found. Adjust names in build_links.py or rebuild ingestion.")

    con.execute("DROP TABLE IF EXISTS electricity_sales_all;")
    con.execute("DROP TABLE IF EXISTS electricity_sales_all_norm;")

    # ---- Clean customer_list_xls ----
    raw = con.sql(f'SELECT * FROM "{t_cust}"').fetchdf()
    cust = promote_header(raw)

    junk_cols = {"techiman", "year_month", "date_only", "source_sheet"}
    cust = cust.drop(columns=[c for c in cust.columns if c.lower() in junk_cols], errors="ignore")

    meter_col = pick_meter_col_from_df(cust)
    cust["meter_no_norm"] = norm14(cust[meter_col])
    cust = cust[cust["meter_no_norm"].ne("")].drop_duplicates("meter_no_norm")

    con.register("customers_norm_df", cust)

    cust_cols = [c for c in cust.columns if c != "meter_no_norm"]
    cust_select = ", ".join([f'c."{c}"' for c in cust_cols]) if cust_cols else "/* no extra customer cols */"

    # ---- Final merged table ----
    con.execute(f"""
        CREATE OR REPLACE TABLE merged_sales_customers AS
        WITH sales_all AS (
            SELECT * FROM "{t_sales_a}"
            UNION ALL
            SELECT * FROM "{t_sales_b}"
        ),
        sales_norm AS (
            SELECT
                s.*,
                RIGHT(LPAD(regexp_replace(CAST(meterid AS VARCHAR), '[^0-9]', ''), 14, '0'), 14) AS meterid_norm
            FROM sales_all s
        )
        SELECT
            s.*,
            c.meter_no_norm AS customer_meter_no_norm
            {"," if cust_cols else ""} {cust_select}
        FROM sales_norm s
        LEFT JOIN customers_norm_df c
          ON s.meterid_norm = c.meter_no_norm;
    """)

    # ---- Stats output requested ----
    stats = con.sql("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(c.meter_no_norm) AS matched,
            COUNT(*) - COUNT(c.meter_no_norm) AS unmatched
        FROM merged_sales_customers m
        LEFT JOIN customers_norm_df c
        ON m.meterid_norm = c.meter_no_norm;
    """).fetchdf().iloc[0]

    total = int(stats["total_rows"])
    matched = int(stats["matched"])
    unmatched = int(stats["unmatched"])
    perc_unmatched = (unmatched / total) * 100 if total else 0

    print(f"Matched meterids     : {matched}")
    print(f"Unmatched meterids   : {unmatched}")
    print(f"Total rows           : {total}")
    print(f"Percent unmatched    : {perc_unmatched:.4f}%")

    con.unregister("customers_norm_df")
    con.close()
    print("Done: merged_sales_customers created.")

if __name__ == "__main__":
    main()

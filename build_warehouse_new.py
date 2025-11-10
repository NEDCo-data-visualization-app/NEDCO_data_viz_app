# warehouse_new.py
# One-time builder: creates 4 tables inside data/warehouse_new.duckdb
#   sales_2012_2017, sales_2018_2019, customer_list_xls, customer_list_xlsx

import pandas as pd
import duckdb
from pathlib import Path

# ---------- config ----------
DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "warehouse_new.duckdb"

FILES = {
    "sales_2012_2017": DATA_DIR / "sales 2012 to 2017.xlsx",
    "sales_2018_2019": DATA_DIR / "Sales 2018 to 2019.xlsx",
    "customer_list_xls": DATA_DIR / "Customer List.xls",
    "customer_list_xlsx": DATA_DIR / "Customer List.xlsx",
}

# Column names that must remain TEXT to avoid scientific-notation damage
ID_COLUMNS = {"METERID", "ORDERSID", "ORDERID", "CUSTOMERID", "CUSTOMER_ID"}

# ---------- helpers ----------
def _id_dtype_map(cols) -> dict:
    # Any column that matches an ID token becomes string dtype on read
    return {c: "string" for c in cols if c.upper() in ID_COLUMNS}

def fix_od_date(df: pd.DataFrame) -> pd.DataFrame:
    if "od_date" not in df.columns:
        return df
    s = df["od_date"]

    # already datetime-like
    if pd.api.types.is_datetime64_any_dtype(s):
        df["od_date"] = pd.to_datetime(s, errors="coerce")
        return df

    # detect per-cell types
    is_ts = s.apply(lambda x: isinstance(x, (pd.Timestamp,)))
    is_num = s.apply(lambda x: isinstance(x, (int, float)))

    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

    # keep real datetimes
    if is_ts.any():
        out.loc[is_ts] = pd.to_datetime(s[is_ts], errors="coerce")

    # convert Excel serials (ints/floats)
    if is_num.any():
        out.loc[is_num] = pd.to_datetime(
            s[is_num].astype(float), unit="d", origin="1899-12-30", errors="coerce"
        )

    # remaining: strings
    remaining = ~(is_ts | is_num)
    if remaining.any():
        s_rem = s[remaining]

        # numeric-looking strings as serials
        s_num = pd.to_numeric(s_rem, errors="coerce")
        mask_serial = s_num.notna()
        if mask_serial.any():
            out.loc[s_num.index[mask_serial]] = pd.to_datetime(
                s_num[mask_serial].astype(float),
                unit="d",
                origin="1899-12-30",
                errors="coerce",
            )

        # leftovers: parse as regular date strings
        leftovers = s_num.index[~mask_serial]
        if len(leftovers) > 0:
            out.loc[leftovers] = pd.to_datetime(s_rem.loc[leftovers], errors="coerce")

    df["od_date"] = out
    return df

def load_excel_all_sheets(path: Path) -> pd.DataFrame:
    xl = pd.ExcelFile(path)
    frames = []
    for sh in xl.sheet_names:
        # peek header to build dtype map for IDs
        header = xl.parse(sh, nrows=0)
        df = xl.parse(sh, dtype=_id_dtype_map(header.columns))
        df.columns = [c.strip().lower() for c in df.columns]
        df["source_sheet"] = sh
        df = fix_od_date(df)
        frames.append(df)
    return pd.concat(frames, ignore_index=True, sort=False)

# ---------- main ----------
def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DB_PATH, read_only=False)

    for name, path in FILES.items():
        if not path.exists():
            print(f"Missing file: {path}")
            continue

        print(f"Loading {path.name} ...")
        df = load_excel_all_sheets(path)
        null_dates = df["od_date"].isna().sum() if "od_date" in df.columns else "n/a"
        print(f"  Rows: {len(df)} | Cols: {len(df.columns)} | date_nulls: {null_dates}")

        # Register DF, then create/replace table from the temp view
        view = f"tmp_{name}"
        con.register(view, df)
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM {view}")
        con.unregister(view)

    con.close()
    print("Done.")

if __name__ == "__main__":
    main()

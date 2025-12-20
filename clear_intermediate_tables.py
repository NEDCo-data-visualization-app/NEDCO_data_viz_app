import duckdb
from pathlib import Path

DB_PATH = Path("data") / "warehouse_new.duckdb"
EXPORT_DIR = Path("parquet_exports")
EXPORT_DIR.mkdir(exist_ok=True)  # Create folder if it doesn't exist

def main():
    # Connect to DuckDB
    con = duckdb.connect(str(DB_PATH), read_only=False)
    
    # Get all tables in the database
    all_tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
    
    print(f"Found tables: {all_tables}")
    
    # Export each table to Parquet
    for table in all_tables:
        parquet_file = EXPORT_DIR / f"{table}.parquet"
        con.execute(f"COPY {table} TO '{parquet_file}' (FORMAT PARQUET)")
        print(f"Exported table '{table}' to: {parquet_file.resolve()}")
    
    con.close()

if __name__ == "__main__":
    main()

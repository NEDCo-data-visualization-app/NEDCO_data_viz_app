import duckdb
import os
import pandas as pd

DUCKDB_PATH = "/Users/srinandham/Downloads/NEDCO_data_viz_app/data/warehouse_new.duckdb"
TABLE_NAME = "merged_sales_customers_clean"
OUTPUT_PARQUET = "/Users/srinandham/Downloads/NEDCO_data_viz_app/data/test.parquet"

con = duckdb.connect(DUCKDB_PATH, read_only=True)

# Export entire table to Parquet
con.execute(f"""
    COPY {TABLE_NAME}
    TO '{OUTPUT_PARQUET}'
    (FORMAT PARQUET);
""")

con.close()


print(f"Parquet written to: {OUTPUT_PARQUET}")

# Read the Parquet file into a Pandas DataFrame
df = pd.read_parquet(OUTPUT_PARQUET)

# Print info (columns, dtypes, non-null counts)
print(df.info())

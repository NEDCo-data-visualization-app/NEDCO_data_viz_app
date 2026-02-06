import duckdb

# Connect to your database in read-only mode if you only want to inspect
con = duckdb.connect(
    database='/Users/srinandham/Downloads/NEDCO_data_viz_app/data/warehouse_new.duckdb',
    read_only=True
)

# Count rows in dashboard_cache
row_count = con.execute("SELECT COUNT(*) FROM predict_all_cache").fetchone()[0]
print(f"predict_all_cache has {row_count} rows")

# Optionally, peek at first few rows
rows = con.execute("SELECT * FROM predict_all_cache LIMIT 5").fetchall()
for row in rows:
    print(row)

con.close()

import duckdb

con = duckdb.connect(database='data/warehouse_new.duckdb', read_only=True)

# Properly get column names
columns = [col[1] for col in con.execute("PRAGMA table_info('predict_all_cache')").fetchall()]
print(columns)

# Optional: see first 5 rows
rows = con.execute("SELECT * FROM predict_all_cache LIMIT 5").fetchall()
for row in rows:
    print(row)

con.close()
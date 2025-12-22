import duckdb

# Path to your DuckDB cache database (adjust if different)
CACHE_DB_PATH = "/Users/srinandham/Downloads/NEDCO_data_viz_app/data/warehouse_new.duckdb"

# Connect in read-only mode
con = duckdb.connect(database=CACHE_DB_PATH, read_only=True)

# Query for all tables in the database
tables = con.execute("SHOW TABLES").fetchall()

print("Tables in cache DB:")
for table in tables:
    print("-", table[0])

# Optional: preview a specific table
table_to_preview = "dashboard_cache"  # the table we wrote first-load data to
rows = con.execute(f"SELECT * FROM {table_to_preview} LIMIT 5").fetchall()
columns = [desc[0] for desc in con.description]

print(f"\nFirst 5 rows of {table_to_preview}:")
for row in rows:
    print(dict(zip(columns, row)))

con.close()

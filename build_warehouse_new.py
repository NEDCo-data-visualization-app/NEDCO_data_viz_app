import duckdb
import pandas as pd

# 1. Create or connect to new database
db = duckdb.connect('data/warehouse_new.duckdb')

# 2. Load and create tables

# Sales 2012–2017
df_12_17 = pd.read_excel('data/Sales 2012 to 2017.xlsx')
db.execute("CREATE TABLE electricity_sales_2012_2017 AS SELECT * FROM df_12_17")

# Sales 2018–2019
df_18_19 = pd.read_excel('data/Sales 2018 to 2019.xlsx')
db.execute("CREATE TABLE electricity_sales_2018_2019 AS SELECT * FROM df_18_19")

# Customer List (.xls format)
df_cust_xls = pd.read_excel('data/Customer List.xls')
db.execute("CREATE TABLE customer_list_xls AS SELECT * FROM df_cust_xls")

# Customer List (.xlsx format)
df_cust_xlsx = pd.read_excel('data/Customer List.xlsx')
db.execute("CREATE TABLE customer_list_xlsx AS SELECT * FROM df_cust_xlsx")

# 3. Done
db.close()
print("✅ warehouse_new.duckdb created with 4 tables.")

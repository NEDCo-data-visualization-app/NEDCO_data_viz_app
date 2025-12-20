# test_bq_env.py
from google.cloud import bigquery
import os
import sys
from dotenv import load_dotenv

load_dotenv() 
def test_bigquery():
    try:
        client = bigquery.Client(location="US")
        table = "volta-test-481721.test123.table_test"
        query = f"SELECT * FROM `{table}` LIMIT 1"
        print(f"Running test query on table: {table}")

        result = client.query(query).result()

        rows = list(result)
        if not rows:
            print("Query succeeded but table is empty.")
        else:
            print("Query succeeded! Sample row:")
            for row in rows:
                print(dict(row))

    except Exception as e:
        print("Failed to query BigQuery:")
        print(e)
        sys.exit(1)

if __name__ == "__main__":
    test_bigquery()

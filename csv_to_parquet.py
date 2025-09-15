import os
import sys
import pandas as pd

def csv_to_parquet(filename: str, data_dir: str = "data") -> None:
    # Ensure input file exists
    csv_path = os.path.join(data_dir, filename)
    if not os.path.isfile(csv_path):
        print(f"❌ File not found: {csv_path}")
        sys.exit(1)

    # Ensure it’s a CSV file
    if not filename.lower().endswith(".csv"):
        print("❌ Please provide a .csv file.")
        sys.exit(1)

    # Load CSV into DataFrame
    df = pd.read_csv(csv_path)

    # Create output parquet path
    base_name = os.path.splitext(filename)[0]
    parquet_path = os.path.join(data_dir, base_name + ".parquet")

    # Save as parquet
    df.to_parquet(parquet_path, index=False)
    print(f"✅ Converted {csv_path} → {parquet_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <filename.csv>")
        sys.exit(1)
    csv_to_parquet(sys.argv[1])

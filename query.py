# query.py
# Run: python query.py
# Optional: set PERSIST_DIR to your data folder (default ".")

import os
from pathlib import Path
import duckdb
import pandas as pd

pd.set_option("display.max_rows", 50)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 180)

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"

def main():
    print(f"DB_FILE = {DB_FILE.resolve()}")

    conn = duckdb.connect(DB_FILE)

    print("\n================ LABOR INVOICE ITEMS SAMPLE ================\n")

    df = conn.execute("""
        SELECT type, name, description
        FROM invoice_items
        WHERE type = 'labor'
        ORDER BY invoice_id DESC
        LIMIT 20;
    """).df()

    print(df)

    conn.close()

    print("\n================ DONE ================\n")

if __name__ == "__main__":
    main()

# drop_all.py
# Run: python drop_all.py
# Optional: set PERSIST_DIR to your data folder (default ".")

import os
from pathlib import Path
import duckdb

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"

TABLES = [
    "jobs",
    "job_employees",
    "employees",
    "customers",
    "invoices",
    "invoice_items",
    "job_appointments",
    "estimates",
    "estimate_options",
    "estimate_employees",
    "metadata",
]

def header(title: str):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)

def main():
    print(f"DB_FILE = {DB_FILE.resolve()}")

    conn = duckdb.connect(DB_FILE)

    # Show current tables
    header("CURRENT TABLES")
    try:
        tables = conn.execute("SHOW TABLES").df()
        print(tables)
    except Exception as e:
        print("Could not list tables:", e)

    header("DROPPING TABLES")

    for t in TABLES:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {t}")
            print(f"Dropped: {t}")
        except Exception as e:
            print(f"Failed to drop {t}: {e}")

    header("FINAL TABLE LIST")
    try:
        tables = conn.execute("SHOW TABLES").df()
        print(tables)
    except Exception as e:
        print("Could not list tables:", e)

    conn.close()

    header("DONE")
    print("Database schema has been fully reset. You can now rerun ingest.py safely.")

if __name__ == "__main__":
    main()

import duckdb
from pathlib import Path
import os
import pandas as pd

pd.set_option("display.max_rows", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", None)

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"

conn = duckdb.connect(DB_FILE)

print("\n=== JOBS SCHEMA ===")
print(conn.execute("DESCRIBE jobs").df())

print("\n=== SAMPLE JOBS ===")
print(
    conn.execute("""
        SELECT *
        FROM jobs
        ORDER BY updated_at DESC
        LIMIT 60
    """).df()
)

print("\n=== Invoices SCHEMA ===")
print(conn.execute("DESCRIBE invoices").df())

print("\n=== SAMPLE JOBS ===")
print(
    conn.execute("""
        SELECT *
        FROM invoices
        LIMIT 60
    """).df()
)

print("\n=== Customers SCHEMA ===")
print(conn.execute("DESCRIBE customers").df())

print("\n=== SAMPLE CUSTOMERS ===")
print(
    conn.execute("""
        SELECT *
        FROM customers
        LIMIT 60
    """).df()
)


print("\n=== ALL DISTINCT TAGS ===")
print(
    conn.execute("""
        SELECT DISTINCT(tags)
        FROM jobs
    """).df()
)

conn.close()

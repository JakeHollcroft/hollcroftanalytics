import duckdb
from pathlib import Path
import os
import pandas as pd

pd.set_option("display.max_rows", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", None)

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"

conn = duckdb.connect(DB_FILE)

# print("\n=== JOBS SCHEMA ===")
# print(conn.execute("DESCRIBE jobs").df())

# print("\n=== SAMPLE JOBS ===")
# print(
#     conn.execute("""
#         SELECT *
#         FROM jobs
#         ORDER BY updated_at DESC
#         LIMIT 60
#     """).df()
# )


print("\n=== ALL DISTINCT TAGS ===")
print(
    conn.execute("""
        SELECT DISTINCT(tags)
        FROM jobs
    """).df()
)

# print("\n=== Distinct Invoice Statuses and Counts ===")
# print(
#     conn.execute("""
#         SELECT status, COUNT(*), sum(amount) from invoices group by status order by 2 desc;
#     """).df()
# )
conn.close()

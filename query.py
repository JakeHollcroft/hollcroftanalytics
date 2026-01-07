import duckdb
from pathlib import Path
import os

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"

conn = duckdb.connect(DB_FILE)

print("\n=== JOBS SCHEMA ===")
print(conn.execute("DESCRIBE jobs").df())

print("\n=== SAMPLE JOBS ===")
print(
    conn.execute("""
        SELECT job_id, work_status, tags, total_amount, updated_at
        FROM jobs
        ORDER BY updated_at DESC
        LIMIT 10
    """).df()
)

conn.close()

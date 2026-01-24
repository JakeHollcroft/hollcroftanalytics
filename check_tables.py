import duckdb
import os
from pathlib import Path

DB = "/var/data/housecall_data.duckdb"
con = duckdb.connect(DB)

print("DB:", DB)
print("\n--- SHOW TABLES ---")
print(con.execute("SHOW TABLES").df())

print("\n--- TABLES LIKE price/book ---")
print(con.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='main'
      AND (lower(table_name) LIKE '%price%' OR lower(table_name) LIKE '%book%')
    ORDER BY table_name
""").df())

con.close()

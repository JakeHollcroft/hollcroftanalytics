import sqlite3
from pathlib import Path

DB_PATH = Path("app.db")
con = sqlite3.connect(DB_PATH)
cur = con.cursor()
cur.execute("ALTER TABLE users ADD COLUMN dashboard_key TEXT")
rows = cur.fetchall()
for row in rows:
    print(row)
con.close()

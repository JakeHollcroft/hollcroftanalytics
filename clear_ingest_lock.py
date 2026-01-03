import duckdb
from clients.jc_mechanical.config import DB_FILE

conn = duckdb.connect(DB_FILE)
conn.execute("DELETE FROM metadata WHERE key = 'ingest_lock'")
conn.close()

print("Ingest lock cleared.")

# worker.py
import time
import duckdb
from clients.jc_mechanical.ingest import run_ingestion
from clients.jc_mechanical.config import DB_FILE

def ensure_data():
    need_ingest = False

    if not DB_FILE.exists():
        print("DB file not found. Running ingestion...")
        need_ingest = True
    else:
        conn = duckdb.connect(DB_FILE)
        try:
            conn.execute("SELECT 1 FROM jobs LIMIT 1").fetchall()
        except duckdb.CatalogException:
            print("Jobs table missing. Running ingestion...")
            need_ingest = True
        finally:
            conn.close()

    if need_ingest:
        run_ingestion()

def scheduler_loop(interval_seconds=3600):
    # optional: delay so deploy finishes before first run
    time.sleep(10)

    while True:
        try:
            print("[WORKER] Starting ingestion...")
            run_ingestion()
            print("[WORKER] Ingestion finished.")
        except Exception as e:
            print(f"[WORKER] Ingestion error: {e}")

        # ALWAYS wait a full interval after completion
        print(f"[WORKER] Sleeping {interval_seconds}s until next run...")
        time.sleep(interval_seconds)

if __name__ == "__main__":
    ensure_data()
    scheduler_loop(interval_seconds=3600)

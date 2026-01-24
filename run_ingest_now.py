"""
Manual ingestion trigger.

Runs the same ingestion logic used by the scheduler,
but can be executed from the shell:

    python run_ingest_now.py
"""

import sys
import traceback
from datetime import datetime

from clients.jc_mechanical.ingest import run_ingestion


def main():
    print("\n[MANUAL] Starting ingestion...")
    print(f"[MANUAL] Timestamp: {datetime.utcnow().isoformat()}")

    try:
        run_ingestion()
        print("\n[MANUAL] Ingestion completed successfully.")
    except Exception as e:
        print("\n[MANUAL] Ingestion FAILED!")
        print(str(e))
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

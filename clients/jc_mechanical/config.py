from pathlib import Path
import os

PERSIST_DIR = Path(os.environ.get("PERSIST_DIR", Path(__file__).resolve().parents[2]))
PERSIST_DIR.mkdir(parents=True, exist_ok=True)

DB_FILE = PERSIST_DIR / "housecall_data.duckdb"

# API key
API_KEY = "2be8ce9fff09493b87c800ceecebe3b1"

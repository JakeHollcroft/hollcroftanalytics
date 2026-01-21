from pathlib import Path
import os

# Choose a consistent persistent directory for BOTH ingestion and dashboard reads.
def _resolve_persist_dir() -> Path:
    env = os.environ.get("PERSIST_DIR")
    if env:
        return Path(env)
    if Path("/var/data").exists():
        return Path("/var/data")
    return Path(__file__).resolve().parents[2]

PERSIST_DIR = _resolve_persist_dir()
PERSIST_DIR.mkdir(parents=True, exist_ok=True)

DB_FILE = PERSIST_DIR / "housecall_data.duckdb"

API_KEY = os.environ.get("HOUSECALLPRO_API_KEY", "").strip()
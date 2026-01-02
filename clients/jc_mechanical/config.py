from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Use Path for DB_FILE
DB_FILE = BASE_DIR / "data" / "housecall_data.duckdb"

# API key
API_KEY = "2be8ce9fff09493b87c800ceecebe3b1"

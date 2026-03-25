import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Database ──────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5433)),
    "dbname": os.getenv("DB_NAME", "trade_db"),
    "user": os.getenv("DB_USER", "daudsaid"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# ── uktradeinfo REST API ───────────────────────────────────────────────────────
API_BASE_URL = "https://api.uktradeinfo.com"

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DOWNLOAD_DIR = BASE_DIR / "data" / "raw"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

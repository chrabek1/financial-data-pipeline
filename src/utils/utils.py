import math
import os
import psycopg2
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
BASE_DELAY = float(os.getenv("BASE_DELAY", 2))
MIN_SYMBOL_DELAY = float(os.getenv("MIN_SYMBOL_DELAY", 1.2))

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

def clean_value(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value
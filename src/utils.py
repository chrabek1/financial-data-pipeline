import math
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "finance_db",
    "user": "finance_user",
    "password": "finance_pass"
}

def clean_value(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value
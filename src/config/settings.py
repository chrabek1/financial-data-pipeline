import os
from dotenv import load_dotenv

load_dotenv()

# DATA SOURCE

DATA_SOURCE = os.getenv("DATA_SOURCE", "alpha_vantage")

ALPHA_VANTAGE_KEY_API = os.getenv("ALPHA_VANTAGE_API_KEY")

# RETRY CONFIGURATION

MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
BASE_DELAY = float(os.getenv("BASE_DELAY", 1.0))
MIN_SYMBOL_DELAY = float(os.getenv("MIN_SYMBOL_DELAY", 1.2))

# DATABASE

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "finance"),
    "user": os.getenv("DB_USER", "finance_user"),
    "password": os.getenv("DB_PASSWORD", "finance_password"),
}
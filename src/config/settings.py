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

# CONNECTION CONFIGURATION

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", 5432))

DB_POOL_MIN=int(os.getenv("DB_POOL_MIN", 1))
DB_POOL_MAX=int(os.getenv("DB_POOL_MAX", 10))
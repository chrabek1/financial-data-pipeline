import os
import requests
import json
import time
import logging
from utils import BRONZE_DIR
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

logger = logging.getLogger(__name__)

def fetch_stock_data(symbol: str) -> dict:
    if not API_KEY:
        raise ValueError("API key not found. Check your .env file.")
    
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": "compact"
    }
    
    logger.info(f"Fetching data for {symbol}...")
    
    response = requests.get(BASE_URL, params=params, timeout=10)
    
    if response.status_code !=200:
        raise Exception(f"HTTP error: {response.status_code}")
    
    data = response.json()
    
    # Check if API returned rate limit message
    if "Note" in data:
        logger.warning("Rate limit reached. Waiting 60 seconds...")
        time.sleep(60)
        return fetch_stock_data(symbol)
    
    if "Error Message" in data:
        raise Exception(f"API error: {data['Error Message']}")
    
    if "Time Series (Daily)" not in data:
        raise ValueError(f"No time series returned for symbol {symbol}")
    return data

def save_raw_data(symbol: str, data:dict):
    today = datetime.now().strftime("%Y-%m-%d")
    filename = BRONZE_DIR / f"{symbol}_{today}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
        
    logger.info(f"Saved raw file to %s", filename)
    return filename
    
def extract_symbol(symbol: str) -> Path:
    stock_data = fetch_stock_data(symbol)
    path = save_raw_data(symbol, stock_data)
    return path
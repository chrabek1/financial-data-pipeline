import os
import requests
import json
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

def fetch_stock_data(symbol: str) -> dict:
    if not API_KEY:
        raise ValueError("API key not found. Check your .env file.")
    
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "aoutputsize": "compact"
    }
    
    print(f"Fetching data for {symbol}...")
    
    response = requests.get(BASE_URL, params=params)
    
    if response.status_code !=200:
        raise Exception(f"HTTP error: {response.status_code}")
    
    data = response.json()
    
    # Check if API returned rate limid message
    if "Note" in data:
        print("Rate limit reached. Waiting 60 seconds...")
        time.sleep(60)
        return fetch_stock_data(symbol)
    
    if "Error Message" in data:
        raise Exception(f"API error: {data['Error Message']}")
    
    return data

def save_raw_data(symbol: str, data:dict):
    project_root = Path(__file__).resolve().parent.parent
    raw_path = project_root / "data" / "raw"
    raw_path.mkdir(parents=True, exist_ok=True)
        
    today = datetime.now().strftime("%Y-%m-%d")
    filename = raw_path / f"{symbol}_{today}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
        
    print(f"saved raw data to {filename}")
    
if __name__ == "__main__":
    symbols = ["AAPL", "MSFT", "TSLA"]
    
    for symbol in symbols:
        stock_data = fetch_stock_data(symbol)
        save_raw_data(symbol, stock_data)
        
        time.sleep(15)
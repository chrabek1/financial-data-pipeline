import json
import pandas as pd
import logging
from utils import SILVER_DIR, BRONZE_DIR
from pathlib import Path

logger = logging.getLogger(__name__)

def load_raw_json(symbol: str):
    files = sorted(BRONZE_DIR.glob(f"{symbol}_*.json"))
    
    if not files:
        raise FileNotFoundError(f"No raw JSON found for {symbol}")
    
    latest_file = files[-1]
    
    with open(latest_file) as f:
        data = json.load(f)
        
    logger.info(f"Loaded raw data from {latest_file}")
    return data

def json_to_dataframe(data: dict) -> pd.DataFrame:
    time_series = data["Time Series (Daily)"]
    
    df = pd.DataFrame.from_dict(time_series, orient="index")
    
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)
    
    df.columns = ["date", "open", "high", "low", "close", "volume"]
    
    df["date"] = pd.to_datetime(df["date"])
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(int)
    
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    df["daily_return"] = (
        (df["close"] - df["close"].shift(1)) / df["close"].shift(1)
    )
    
    df["rolling_avg_7"] = df["close"].rolling(window=7).mean()
    df["volatility_7"] = df["close"].rolling(window=7).std()
    
    logger.info(f"Transformed data into DataFrame with {len(df)} rows")
    return df


def transform_symbol(symbol: str) -> Path:
    logger.info(f"Starting transform for {symbol}")
    
    raw_data = load_raw_json(symbol)
    df = json_to_dataframe(raw_data)
    
    df = df.where(pd.notnull(df), None)
    
    return df
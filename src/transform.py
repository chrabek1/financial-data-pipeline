import json
import pandas as pd
from pathlib import Path


def load_raw_json(symbol: str):
    project_root = Path(__file__).resolve().parent.parent
    raw_path = project_root / "data" / "raw"
    
    files = sorted(raw_path.glob(f"{symbol}_*.json"))
    latest_file = files[-1]
    
    with open(latest_file) as f:
        data = json.load(f)
        
    return data

def json_to_dataframe(data: dict):
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
      
    return df

def save_processed_data(symbol: str, df: pd.DataFrame):
    project_root = Path(__file__).resolve().parent.parent
    processed_path = project_root / "data" / "processed"
    processed_path.mkdir(parents=True, exist_ok=True)
    
    filename = processed_path / f"{symbol}_processed.csv"
    
    df.to_csv(filename, index=False)
    
    print(f"Saved processed data to {filename}")

if __name__ == "__main__":
    symbol = "AAPL"
    
    raw_data = load_raw_json(symbol)
    df = json_to_dataframe(raw_data)
    
    save_processed_data(symbol,df)
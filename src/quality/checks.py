def validate_prices(df):
    
    if (df["high"] < df["low"]).any():
        raise ValueError("Invalid price data: high < low")
    
    if (df["volume"] < 0).any():
        raise ValueError("Invalid volume")
    
    if df.duplicated(["symbol", "date"]).any():
        raise ValueError("Duplicate rows")
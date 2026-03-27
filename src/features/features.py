import pandas as pd

def add_features(context, df: pd.DataFrame) -> pd.DataFrame:
    
    df["daily_return"] = (
        (df["close"] - df["close"].shift(1)) / df["close"].shift(1)
    )
    df["rolling_avg_7"] = df["close"].rolling(window=7).mean()
    df["volatility_7"] = df["close"].rolling(window=7).std()
    
    df = df.where(pd.notnull(df), None)

    return df

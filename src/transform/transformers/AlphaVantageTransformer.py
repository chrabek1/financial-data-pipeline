import pandas as pd
from .base import BaseTransformer

class AlphaVantageTransformer(BaseTransformer):
    
    def transform(self, payload: dict, symbol: str):
    
        ts = payload["Time Series (Daily)"]
        
        records = []
        
        for date, values in ts.items():
            records.append({
                "symbol": symbol,
                "date": date,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"])
            })
            
        df = pd.DataFrame(records)
        
        df["date"] = pd.to_datetime(df["date"])
        
        df.sort_values("date", inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        return df

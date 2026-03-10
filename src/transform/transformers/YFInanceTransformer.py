import pandas as pd
from .base import BaseTransformer

class YFinanceTransformer(BaseTransformer):
    
    def transform(self, payload, symbol):
        
        df = pd.DataFrame(payload)

        df["symbol"] = symbol

        df["date"] = pd.to_datetime(df["date"])

        df = df[
            ["symbol", "date", "open", "high", "low", "close", "volume"]
        ]

        return df
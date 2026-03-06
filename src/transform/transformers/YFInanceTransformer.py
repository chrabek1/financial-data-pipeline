import pandas
from .base import BaseTransformer

class YFinanceTransformer(BaseTransformer):
    
    def transform(self, payload, symbol):
        
        df = payload.copy()
        
        df["symbol"] = symbol
        
        df.rename(colums={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }, inplace=True)
        return df
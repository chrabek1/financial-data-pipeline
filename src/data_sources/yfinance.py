import yfinance as yf
from .base import DataSource

class YFinanceSource(DataSource):
    def fetch_daily(self, symbol: str) -> dict:
        df = yf.download(symbol, period="100d")
        
        if df.empty:
            raise ValueError("No data returned")
        
        return df.to_dict()
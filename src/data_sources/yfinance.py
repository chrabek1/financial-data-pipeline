import yfinance as yf
from .base import DataSource

class YFinanceSource(DataSource):
    def fetch_daily(self, symbol: str) -> dict:
        df = yf.download(symbol, period="100d")
        
        if df.empty:
            raise ValueError("No data returned")
        
        df.columns = [str(col).split(",")[0].replace("(", "").replace(")", "").replace("'", "") for col in df.columns]
        df.columns = [c.lower() for c in df.columns]
            
        df = df.reset_index()
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
        
        df.columns = [c.lower() for c in df.columns]
        
        return df.to_dict(orient="records")
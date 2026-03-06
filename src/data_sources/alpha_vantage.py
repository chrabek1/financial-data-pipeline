import requests
import logging
from .base import DataSource
from exceptions import ExtractTransientError

logger = logging.getLogger(__name__)

class AlphaVantageSource(DataSource):
    
    def __init__(self, api_key: str) -> dict:
        self.api_key = api_key
        
    def fetch_daily(self, symbol: str) -> dict:
        url = (
            "https://www.alphavantage.co/query"
            f"?function=TIME_SERIES_DAILY"
            f"&symbol={symbol}"
            f"&apikey={self.api_key}"
        )
        
        response = requests.get(url)
        data = response.json()
        
        # logger.debug("API response keys: %s", data.keys())        
        
        if "Information" in data:
            raise ExtractTransientError(data["Information"])
        
        if "Error Message" in data:
            raise ValueError(data["Error Message"])
        
        if "Time Series (Daily)" not in data:
            raise ValueError("No time series returned")
        
        return data
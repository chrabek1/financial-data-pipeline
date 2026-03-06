import os
from data_sources.alpha_vantage import AlphaVantageSource
from data_sources.yfinance import YFinanceSource

def get_data_source():
    source_type = os.getenv("DATA_SOURCE", "alpha_vantage")
    
    if source_type == "alpha_vantage":
        api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        return AlphaVantageSource(api_key)
    
    elif source_type == "yfinance":
        return YFinanceSource()
    
    else:
        raise ValueError(f"Unknown DATA_SOURCE: {source_type}")
from .AlphaVantageTransformer import AlphaVantageTransformer
from .YFInanceTransformer import YFinanceTransformer

class TransformFactory:
    
    @staticmethod
    def get_transformer(source: str):
        
        if source == "alpha_vantage":
            return AlphaVantageTransformer()
        
        if source == "yfinance":
            return YFinanceTransformer()
        
        raise ValueError(f"Unknown source: {source}")
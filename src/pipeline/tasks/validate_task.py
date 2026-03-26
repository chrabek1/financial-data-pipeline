from models.stock_daily import StockDailyModel
import pandas as pd 

def validate_task(df: pd.DataFrame) -> pd.DataFrame:
    
    df = StockDailyModel.enforce_types(df)
    StockDailyModel.validate_full(df)
    
    return df
from transform.transform import transform_symbol
from models.stock_daily import StockDailyModel

def transform_task(path):
    df = transform_symbol(path)
    
    df = StockDailyModel.enforce_types(df)
    StockDailyModel.validate_base(df)
    StockDailyModel.validate_business_rules(df)

    
    return df
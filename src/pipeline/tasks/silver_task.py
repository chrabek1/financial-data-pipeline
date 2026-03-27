from utils.paths import SILVER_DIR
from pipeline.context import SymbolContext

def save_silver_task(context: SymbolContext, df):
    
    symbol=context.symbol
    
    silver_dir = SILVER_DIR / symbol
    silver_dir.mkdir(parents=True, exist_ok=True)
    
    silver_file = silver_dir / f"{symbol}.parquet"
    
    df.to_parquet(silver_file, engine="pyarrow", index=False)
    
    return silver_file
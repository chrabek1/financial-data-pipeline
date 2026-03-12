from utils.paths import SILVER_DIR

def save_silver_task(df, symbol):
    
    silver_dir = SILVER_DIR / symbol
    silver_dir.mkdir(parents=True, exist_ok=True)
    
    silver_file = silver_dir / f"{symbol}.parquet"
    
    df.to_parquet(silver_file, engine="pyarrow", index=False)
    
    return silver_file
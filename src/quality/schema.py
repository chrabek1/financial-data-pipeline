import pandas as pd

REQUIRED_COLUMNS = {
    "symbol": "object",
    "date": "datetime64[ns]",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "int64"
}

def validate_schema(df: pd.DataFrame):
    
    for col, dtype in REQUIRED_COLUMNS.items():
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
        
        if str(df[col].dtype) != dtype:
            raise ValueError(
                f"Invalid dtype fpr {col}: expected {dtype}, got {df[col].dtype}"
            )

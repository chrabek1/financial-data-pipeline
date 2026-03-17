import pandas as pd

class StockDailyModel:
    """
    Data contract dla stock daily dataset.
    To jest jedyne źródło prawdy o danych.
    """
    
    COLUMNS = [
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "daily_return",
        "rolling_avg_7",
        "volatility_7",
    ]
    
    DTYPES = {
        "symbol": "object",
        "date": "datetime64[ns]",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "int64",
        "daily_return": "float64",
        "rolling_avg_7": "float64",
        "volatility_7": "float64",
    }
    
    @classmethod
    def validate(cls, df: pd.DataFrame) -> None:
        # 1. kolumny
        missing = set(cls.COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        
        # 2. typy
        for col, expected_dtype in cls.DTYPES.items():
            if col in df.columns:
                if str(df[col].dtype) != expected_dtype:
                    raise ValueError(
                        f"Invalid dtype for {col}: expected {expected_dtype}, got {df[col].dtype}"
                    )
    
    @classmethod
    def enforce(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Wymusza schema -- konwertuje typy.
        """
        df = df.copy()
        
        for col, dtype in cls.DTYPES.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)
        
        return df
        
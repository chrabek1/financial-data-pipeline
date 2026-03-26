import pandas as pd

class StockDailyModel:
    """
    Data contract dla stock daily dataset.
    To jest jedyne źródło prawdy o danych.
    """
    
    BASE_COLUMNS = [
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    
    FULL_COLUMNS =  BASE_COLUMNS + [
        "daily_return",
        "rolling_avg_7",
        "volatility_7",
    ]
    
    NOT_NULL_COLUMNS = [
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
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
    def _validate_dtypes(cls, df: pd.DataFrame) -> None:
        for col, expected_dtype in cls.DTYPES.items():
            if col in df.columns:
                actual_dtype = str(df[col].dtype)
                if actual_dtype != expected_dtype:
                    raise ValueError(
                        f"Invalid dtype for '{col}': expected '{expected_dtype}', get '{actual_dtype}'"
                    )
    
    @classmethod
    def _validate_no_unexpected_columns(cls, df: pd.DataFrame, allowed: list) -> None:
        unexpected = set(df.columns) - set(allowed)
        if unexpected:
            raise ValueError(f"Unexpected columns present: {unexpected}")
    
    @classmethod
    def validate_base(cls, df: pd.DataFrame) -> None:
        missing = set(cls.BASE_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        
        cls._validate_no_unexpected_columns(df, cls.BASE_COLUMNS)
        
        cls._validate_dtypes(df)
        
        #not null check
        null_cols = [col for col in cls.NOT_NULL_COLUMNS if df[col].isnull().any()]
        if null_cols:
            raise ValueError (f"Null values found in required columns: {null_cols}")

    @classmethod
    def validate_full(cls, df: pd.DataFrame) -> None:
        missing = set(cls.FULL_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        
        cls._validate_no_unexpected_columns(df, cls.FULL_COLUMNS)
        
        cls._validate_dtypes(df)
        
        # not null check
        null_cols = [col for col in cls.NOT_NULL_COLUMNS if df[col].isnull().any()]
        if null_cols:
            raise ValueError (f"Null values found in required columns: {null_cols}")
        
        cls.validate_business_rules(df)
        
    @classmethod
    def validate_business_rules(cls, df):
        if (df["close"] < 0).any():
            raise ValueError("Invalid close price: negative values detected")
        if (df["volume"] < 0).any():
            raise ValueError("Invalid volume: negative values detected")
        if ((df["high"] < df["low"]) | (df["open"] < 0) | (df["low"] < 0)).any():
            raise ValueError("Invalid OHLC values detected")
    
    @classmethod
    def enforce_types(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Wymusza schema -- konwertuje typy zgodnie z kontraktem.
        """
        df = df.copy()
        
        for col, dtype in cls.DTYPES.items():
            if col not in df.columns:
                continue
            
            if dtype.startswith("datetime"):
                df[col] = pd.to_datetime(df[col], errors="raise")
            elif dtype.startswith(("float","int")):
                df[col] = pd.to_numeric(df[col], errors="raise").astype(dtype)
            else:
                df[col] = df[col].astype(dtype)
        
        return df
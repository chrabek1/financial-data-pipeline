from datetime import timedelta
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_max_date_for_symbol(cur, symbol: str) -> pd.Timestamp | None:
    cur.execute(
        """
        SELECT MAX(d.full_date)
        FROM fact_stock_daily f
        JOIN dim_stock s ON f.stock_id = s.stock_id
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE s.symbol = %s
        """,
        (symbol,),
    )
    
    result = cur.fetchone()[0]
    return pd.to_datetime(result) if result else None

def filter_refresh_window(df: pd.DataFrame, max_date: pd.Timestamp | None, symbol: str):
    if not max_date:
        return df, None
    
    refresh_from = max_date - timedelta(days=7)
    
    logger.info(
        "Window refresh from %s for %s",
        refresh_from.date(),
        symbol,
    )
    
    df = df[df["date"] >= refresh_from]
    
    logger.info("Filtered to %d new rows", len(df))
    
    return df, refresh_from

def delete_refresh_window(cur, symbol: str, refresh_from):
    if not refresh_from:
        return
    
    cur.execute(
        """
        DELETE FROM fact_stock_daily f
        USING dim_stock s, dim_date d
        WHERE f.stock_id = d.date_id
        AND f.date_id = d.date_id
        AND s.symbol = %s
        AND d.full_date >= %s
        """,
        (symbol, refresh_from),
    )
    
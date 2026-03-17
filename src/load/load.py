import pandas as pd
import logging
from load.dimensions import load_dimensions
from load.staging import insert_staging
from load.facts import load_fact_table
from load.incremental import (
    get_max_date_for_symbol,
    filter_refresh_window,
    delete_refresh_window
)

logger = logging.getLogger(__name__)


def batch_already_loaded(cur, symbol: str, batch_id: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM fact_stock_daily f
        JOIN dim_stock s ON f.stock_id = s.stock_id
        WHERE f.batch_id = %s
        AND s.symbol = %s
        LIMIT 1
        """,
        (batch_id, symbol),
    )
    return cur.fetchone() is not None


def prepare_dateframe(df: pd.DataFrame, symbol: str, batch_id: str) -> pd.DataFrame:
    df_copy = df.copy()
    
    df_copy["batch_id"] = str(batch_id)
    df_copy["symbol"] = symbol
    
    df_copy = df_copy[
        [
            "batch_id",
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
    ]
    
    return df_copy


def load_symbol(cur, symbol: str, df: pd.DataFrame, batch_id: str) -> int:
    logger.info("Starting load for %s (batch_id=%s)", symbol, batch_id)
    
    # IMPODENCY
    if batch_already_loaded(cur, symbol, batch_id):
        logger.info(
            "Batch %s for symbol %s already loaded. Skipping load.",
            batch_id,
            symbol,
        )
        return 0
    
    # INCREMENTAL
    max_date = get_max_date_for_symbol(cur, symbol)
    df_filtered, refresh_from = filter_refresh_window(df, max_date, symbol)
    
    if df_filtered.empty:
        logger.info("No new data to load for %s", symbol)
        return 0
    
    #PREPARE
    df_prepared = prepare_dateframe(df_filtered, symbol, batch_id)
    
    #CLEAN OLD DATA
    delete_refresh_window(cur, symbol, refresh_from)
    
    #LOAD PIPELINE
    insert_staging(cur, df_prepared, batch_id)
    load_dimensions(cur, batch_id)
    rows_loaded = load_fact_table(cur, batch_id)
    
    logger.info("Load completed for %s (batch_id=%s)", symbol, batch_id)
    
    return rows_loaded
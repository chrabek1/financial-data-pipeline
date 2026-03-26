import pandas as pd
import logging
from typing import Optional
from load.incremental import get_max_date_for_symbol, filter_refresh_window, delete_refresh_window
from load.staging import insert_staging
from load.dimensions import load_dimensions
from load.facts import load_fact_table
from models.stock_daily import StockDailyModel

logger = logging.getLogger(__name__)

class StockLoader:
    
    def __init__(self, cur, symbol: str, batch_id: str):
        self.cur = cur
        self.symbol = symbol
        self.batch_id = batch_id
    
    def _filter_incremental(self, df: pd.DataFrame) -> tuple[pd.DataFrame, Optional[pd.Timestamp]]:
        
        max_date = get_max_date_for_symbol(self.cur, self.symbol)
        df_filtered, refresh_from = filter_refresh_window(df, max_date, self.symbol)
        
        return df_filtered, refresh_from
    
    
    def _apply_refresh_window(self, refresh_from: Optional[pd.Timestamp]) -> None:
        if refresh_from is not None:
            delete_refresh_window(self.cur, self.symbol, refresh_from)
    
    
    def _load_staging(self,df: pd.DataFrame) -> None:
        logger.info("Staging load start (batch_id=%s)", self.batch_id)
        insert_staging(self.cur, df, self.batch_id)
    
    def _load_dimensions(self) -> None:
        logger.info("Dimensions load start (batch_id=%s)", self.batch_id)
        load_dimensions(self.cur, self.batch_id)
        
    def _load_fact(self) -> int:
        logger.info("Fact load start (batch_id=%s)", self.batch_id)
        return load_fact_table(self.cur, self.batch_id)
    
    def _persist(self, df: pd.DataFrame) -> int:
        
        self._load_staging(df)
        self._load_dimensions()
        rows_loaded = self._load_fact()
        
        return rows_loaded
    
    
    def _batch_already_loaded(self) -> bool:
        self.cur.execute(
            """
            SELECT 1
            FROM fact_stock_daily 
            WHERE batch_id = %s
            AND stock_id = (
                SELECT stock_id FROM dim_stock WHERE symbol = %s
            )
            LIMIT 1
            """,
            (self.batch_id, self.symbol),
        )
        return self.cur.fetchone() is not None
    
    def _prepare_for_load(self, df: pd.DataFrame) -> pd.DataFrame:
        df_copy = df.copy()
        
        df_copy["batch_id"] = str(self.batch_id)
        
        columns = ["batch_id"] + StockDailyModel.FULL_COLUMNS
        
        missing = set(columns) - set(df_copy.columns)
        if missing:
            raise ValueError(f"Missing columns for load: {missing}")
        
        return df_copy[columns]
    
    def load(self, df: pd.DataFrame) -> int:
        
        logger.info("Starting load for %s (batch_id=%s)", self.symbol, self.batch_id)
        
        if self._batch_already_loaded():
            logger.info(
                "Batch %s for symbol %s already loaded. Skipping load.",
                self.batch_id,
                self.symbol,
            )
            return 0
        
        df = self._prepare_for_load(df)
        
        df, refresh_from = self._filter_incremental(df)
        
        if df.empty:
            logger.info("No new data to load for %s", self.symbol)
            return 0
        
        self._apply_refresh_window(refresh_from)
        
        rows_loaded = self._persist(df)
        
        logger.info("Load completed for %s (batch_id=%s)", self.symbol, self.batch_id)
        
        return rows_loaded
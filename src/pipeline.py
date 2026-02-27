import logging
import sys
import uuid
import time
from extract import extract_symbol, ExtractTransientError
from transform import transform_symbol
from load import load_symbol
from utils import get_connection, MAX_RETRIES, BASE_DELAY, MIN_SYMBOL_DELAY
from audit import *
from retry import retry

logger = logging.getLogger(__name__)

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    
def process_symbol(conn, batch_id: str, symbol: str) -> None:
        logger.info(
            "Processing symbol %s (batch_id=%s)",
            symbol,
            batch_id
        )
        try:
            with conn.cursor() as cur:
                create_symbol_record(cur, batch_id, symbol)
            conn.commit()
            
            retry(
                lambda: extract_symbol(symbol, batch_id),
                max_retries=MAX_RETRIES,
                base_delay=BASE_DELAY,
                exceptions=(ExtractTransientError,)
            )
            
            df = transform_symbol(symbol)
            
            with conn.cursor() as cur:                
                rows_loaded = load_symbol(cur, symbol, df, batch_id)
                mark_symbol_success(cur, batch_id, symbol, rows_loaded)
            conn.commit()
            
            logger.info("Symbol %s SUCCESS", symbol)

        except Exception as e:
            conn.rollback()
            
            with conn.cursor() as cur:
                mark_symbol_failed(cur, batch_id, symbol, str(e))
            conn.commit()
            
            logger.error(
                "Symbol %s failed in batch %s: %s",
                symbol,
                batch_id,
                str(e)
            )

def run_pipeline(symbols: list[str]) -> None:
    
    batch_id = str(uuid.uuid4())
    total_symbols = len(symbols)
    
    logger.info(
        "Starting batch %s with %d symbols",
        batch_id,
        total_symbols
    )
    
    conn = get_connection()
    
    with conn.cursor() as cur:
        create_batch(cur, batch_id, total_symbols)
    conn.commit()

    
    for symbol in symbols:
        logger.info("Starting pipeline for %s", symbol)
        process_symbol(conn,batch_id,symbol)
        time.sleep(MIN_SYMBOL_DELAY)
        
    with conn.cursor() as cur:
        mark_batch_status(cur, batch_id)
    conn.commit()
        
    conn.close()
    
    logger.info("Batch %s finalized", batch_id)
    
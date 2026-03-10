import logging
import os
import uuid
import time
from extract.extract import extract_symbol
from exceptions import ExtractTransientError
from transform.transform import transform_symbol
from features.features import add_features
from load.load import load_symbol
from utils.db import get_connection
from utils.paths import SILVER_DIR
from audit.audit import *
from retry import retry
from quality.checks import validate_prices
from config.settings import MAX_RETRIES, BASE_DELAY, MIN_SYMBOL_DELAY
from quality.schema import validate_schema
from pipeline.tasks import run_task
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2

logger = logging.getLogger(__name__)

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    


def process_symbol(batch_id: str, symbol: str) -> None:
    
    conn = get_connection()
    
    logger.info("Processing symbol %s (batch_id=%s)", symbol, batch_id)
    
    try:
        
        with conn.cursor() as cur:
            create_symbol_record(cur, batch_id, symbol)
        conn.commit()
    
        # EXTRACT
        path = retry(
            lambda: run_task("extract", extract_symbol, symbol, batch_id),
            max_retries=MAX_RETRIES,
            base_delay=BASE_DELAY,
            exceptions=(ExtractTransientError,)
        )
        
        logger.info("Extract completed for %s -> %s", symbol, path)
        
        # TRANSFORM
        df = transform_symbol(path)
        
        # VALIDATION
        validate_schema(df)
        validate_prices(df)
        
        # SAVE SILVER
        silver_dir = SILVER_DIR / symbol
        silver_dir.mkdir(parents=True, exist_ok=True)
        
        silver_file = silver_dir / f"{symbol}.parquet"
        
        df.to_parquet(silver_file, engine="pyarrow", index=False)
        
        logger.info("Saved silver dataset to %s", silver_file)
        
        # FEATURES
        df = add_features(df)
        
        # LOAD
        with conn.cursor() as cur:
            rows_loaded = retry(
                lambda: run_task("load", load_symbol, cur, symbol, df, batch_id),
                max_retries=MAX_RETRIES,
                base_delay=BASE_DELAY,
                exceptions=(psycopg2.OperationalError,)
            )
            
            mark_symbol_success(cur, batch_id, symbol, rows_loaded)
        
        conn.commit()
        
        logger.info("Symbol %s SUCCESS", symbol)
    
    except Exception as e:
        
        conn.rollback()
        
        with conn.cursor() as cur:
            mark_symbol_failed(cur, batch_id, symbol, str(e))
        conn.commit()
        
        logger.error("Symbol %s failed in batch %s: %s", symbol, batch_id, str(e))
        
    finally:
    
        conn.close()
    
    

def run_pipeline(symbols: list[str]) -> None:
    
    if not symbols:
        raise ValueError("No symbols provided")
    
    batch_id = str(uuid.uuid4())
    total_symbols = len(symbols)
    
    logger.info(
        "Starting batch %s with %d symbols",
        batch_id,
        total_symbols
    )
    # BATCH START
    
    conn = get_connection()
    
    with conn.cursor() as cur:
        create_batch(cur, batch_id, total_symbols)
    conn.commit()
    conn.close()
    
    # PARALLEL PROCESSING
    
    with ThreadPoolExecutor(max_workers=min(4, len(symbols))) as executor:
        futures = []
        
        for symbol in symbols:
            logger.info("Submitting symbol %s to executor", symbol)
            futures.append(
                executor.submit(process_symbol, batch_id, symbol)
            )
        for future in as_completed(futures):
            future.result()

    # FINALIZE BATCH
    
    conn = get_connection()
    
    with conn.cursor() as cur:
        mark_batch_status(cur, batch_id)
    conn.commit()
        
    conn.close()
    
    logger.info("Batch %s finalized", batch_id)
    
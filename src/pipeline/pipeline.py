import logging
import uuid
from features.features import add_features
from utils.db import get_connection, release_connection
from audit.audit import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline.tasks.extract_task import extract_task
from pipeline.tasks.transform_task import transform_task
from pipeline.tasks.silver_task import save_silver_task
from pipeline.tasks.load_task import load_task
from pipeline.tasks.run_task import run_task
from pipeline.tasks.validate_task import validate_task
from pipeline.context import SymbolContext

logger = logging.getLogger(__name__)

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    
def process_symbol(context: SymbolContext) -> None:
    
    conn = get_connection()
    
    symbol = context.symbol
    batch_id = context.batch_id
    
    logger.info("Processing symbol %s (batch_id=%s)", symbol, batch_id)
    
    try:
        
        with conn.cursor() as cur:
            create_symbol_record(cur, context)
        conn.commit()
    
        # EXTRACT
        path = run_task("extract", extract_task, context)  
        
        # TRANSFORM
        df = run_task("transform", transform_task,context, path)
        
        # SAVE SILVER
        run_task("silver", save_silver_task, context, df)
        
        # FEATURES
        df = run_task("features", add_features, df)
        
        # MODEL VALIDATION
        df = run_task("validate", validate_task, df)
                
        # LOAD
        with conn.cursor() as cur:
            
            rows_loaded = run_task("load", load_task, cur, context, df)
            
            mark_symbol_success(cur, context, rows_loaded)
        
        conn.commit()
        
        logger.info("Symbol %s SUCCESS", symbol)

    
    except Exception as e:
        
        conn.rollback()
        
        with conn.cursor() as cur:
            mark_symbol_failed(cur, context, str(e))
        conn.commit()
        
        logger.error("Symbol %s failed in batch %s: %s", symbol, batch_id, str(e))
        
    finally:
    
        release_connection(conn)
    
    

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
                executor.submit(process_symbol, SymbolContext(symbol, batch_id))
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
    
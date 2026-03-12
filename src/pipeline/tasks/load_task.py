import psycopg2
from utils.retry import retry
from config.settings import MAX_RETRIES, BASE_DELAY
from load.load import load_symbol
from pipeline.tasks.run_task import run_task

def load_task(cur, symbol, df, batch_id):
    
    rows_loaded = retry(
        lambda: run_task("load", load_symbol, cur, symbol, df, batch_id),
        max_retries=MAX_RETRIES,
        base_delay=BASE_DELAY,
        exceptions=(psycopg2.OperationalError,)
    )
        
    return rows_loaded
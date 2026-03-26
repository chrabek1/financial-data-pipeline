import psycopg2
from utils.retry import retry
from config.settings import MAX_RETRIES, BASE_DELAY
from load.loader import StockLoader
from pipeline.tasks.run_task import run_task
from pipeline.context import SymbolContext



def load_task(cur, context: SymbolContext, df) -> int:
    
    symbol = context.symbol
    batch_id = context.batch_id

    loader = StockLoader(cur, symbol, batch_id)
    
    return  retry(
        lambda: loader.load(df),
        max_retries=MAX_RETRIES,
        base_delay=BASE_DELAY,
        exceptions=(psycopg2.OperationalError,)
    )
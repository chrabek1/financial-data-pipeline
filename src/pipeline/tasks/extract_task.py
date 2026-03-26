from utils.retry import retry
from config.settings import MAX_RETRIES, BASE_DELAY
from exceptions import ExtractTransientError
from extract.extract import extract_symbol
from pipeline.context import SymbolContext

def extract_task(context: SymbolContext):
    
    symbol=context.symbol
    batch_id=context.batch_id
    
    return retry(
        lambda: extract_symbol(symbol, batch_id),
        max_retries=MAX_RETRIES,
        base_delay=BASE_DELAY,
        exceptions=(ExtractTransientError, )
    )
from utils.retry import retry
from config.settings import MAX_RETRIES, BASE_DELAY
from exceptions import ExtractTransientError
from extract.extract import extract_symbol
from pipeline.tasks.run_task import run_task

def extract_task(symbol, batch_id):
    
    return retry(
        lambda: run_task("extract", extract_symbol, symbol, batch_id),
        max_retries=MAX_RETRIES,
        base_delay=BASE_DELAY,
        exceptions=(ExtractTransientError, )
    )
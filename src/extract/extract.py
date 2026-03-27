import json
import logging
import os 
from pathlib import Path
from datetime import datetime
from config.settings import DATA_SOURCE
from utils.paths import BRONZE_DIR
from data_sources.factory import get_data_source
from exceptions import ExtractTransientError


logger = logging.getLogger(__name__)

data_source = get_data_source()

def extract_symbol(symbol: str, batch_id:str) -> Path:
    
    logger.debug("Fetching data for %s using %s", symbol, DATA_SOURCE)
    
    payload = data_source.fetch_daily(symbol)
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
    batch_dir = BRONZE_DIR / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = batch_dir / f"{symbol}.json"
    
    bronze_record = {
        "symbol": symbol,
        "batch_id": batch_id,
        "source": DATA_SOURCE,
        "fetched_at": timestamp,
        "payload": payload
    }
    
    with open(file_path,"w") as f:
        json.dump(bronze_record, f, indent=2)
        
    logger.debug("Saved raw file to %s", file_path)
    
    return file_path
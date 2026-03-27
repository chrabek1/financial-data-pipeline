import json
import logging
from pathlib import Path
from models.stock_daily import StockDailyModel
from pipeline.context import SymbolContext

from .transformers.factory import TransformFactory

logger = logging.getLogger(__name__)

def transform_symbol(context: SymbolContext, path: Path):
    
    logger.debug("Starting transform for %s", path)
    
    with open(path) as f:
        raw = json.load(f)
        
        
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid JSON structure in {path}")
        
    required_fields = ["source", "symbol", "payload"]
    
    for field in required_fields:
        if field not in raw:
            raise ValueError(f"Missing field '{field}' in bronze file {path}")
        
    expected_symbol = context.symbol
        
    source = raw["source"]
    symbol = raw["symbol"]
    payload = raw["payload"]
    
    if not symbol:
        raise ValueError(f"Empty symbol in {path}")
    
    if not isinstance(payload, list):
        raise ValueError(f"Payload must be a list in {path}")
    
    if symbol != expected_symbol:
        raise ValueError(
            f"Symbol mismatch: expected {expected_symbol}, got {symbol} in {path}"
        )
    
    transformer = TransformFactory.get_transformer(source)
    
    df = transformer.transform(payload, symbol)
    df = df[StockDailyModel.BASE_COLUMNS]
    
    logger.debug("Transformed data into DataFrame with %s rows", len(df))
    
    return df
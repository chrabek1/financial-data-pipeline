import json
import logging
from pathlib import Path

from .transformers.factory import TransformFactory

logger = logging.getLogger(__name__)

def transform_symbol(path: Path):
    
    logger.info("Starting transform for %s", path)
    
    with open(path) as f:
        raw = json.load(f)
        
    required_fields = ["source", "symbol", "payload"]
    
    for field in required_fields:
        if field not in raw:
            raise ValueError(f"Missing field '{field}' in bronze file {path}")
        
    source = raw["source"]
    symbol = raw["symbol"]
    payload = raw["payload"]
    
    transformer = TransformFactory.get_transformer(source)
    
    df = transformer.transform(payload, symbol)
    
    logger.info("Transformed data into DataFrame with %s rows", len(df))
    
    return df
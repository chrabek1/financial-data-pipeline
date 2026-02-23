import logging
import sys
from extract import extract_symbol
from transform import transform_symbol
from load import load_symbol


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    
    
def run_pipeline(symbol: str) -> None:
    logger = logging.getLogger(__name__)
    logger.info(f"Starting pipeline for %s", symbol)
    
    try:
        extract_symbol(symbol)
        df = transform_symbol(symbol)
        load_symbol(symbol, df)
        
        logger.info("Pipeline completed successfully for %s", symbol)
        
    except Exception as e:
        logger.error("Pipeline failed for %s: %s", symbol, e)
        raise
    
if __name__ == "__main__":
    configure_logging()
    
    if len(sys.argv) < 2:
        print("Usage: python pipeline.py SYMBOL")
        sys.exit(1)
        
    for symbol in sys.argv[1:]:
        run_pipeline(symbol)
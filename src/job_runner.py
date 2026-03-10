import logging
import sys
from pipeline.pipeline import run_pipeline


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )


if __name__ == "__main__":
    configure_logging()

    if len(sys.argv) < 2:
        print("Usage: python job_runner.py AAPL TSLA MSFT")
        sys.exit(1)

    symbols = sys.argv[1:]

    run_pipeline(symbols)
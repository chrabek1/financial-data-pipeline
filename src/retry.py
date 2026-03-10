import time
import logging
import random



logger = logging.getLogger(__name__)

def retry(
    func,
    max_retries: int = 3,
    base_delay: float = 2.0,
    exceptions: tuple =(Exception, )
):
    delay = base_delay
    
    for attempt in range(1, max_retries + 1):
        try:
            return func()
        
        except exceptions as e:
            logger.warning(
                "Retry attempt %d failed: %s",
                attempt,
                str(e)
            )
            
            if attempt == max_retries:
                logger.error("Max retries reached. Raising exception.")
                raise
            
            sleep_time = delay + random.uniform(0, 0.5)

            logger.info(
                "Sleeping %.2f seconds before retry",
                sleep_time
            )

            time.sleep(sleep_time)

            delay *= 2
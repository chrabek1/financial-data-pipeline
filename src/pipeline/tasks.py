import logging

logger = logging.getLogger(__name__)

def run_task(name, func, *args, **kwargs):
    logger.info("Starting task: %s", name)
    
    result = func(*args, **kwargs)
    
    logger.info("Finished task: %s", name)
    
    return result
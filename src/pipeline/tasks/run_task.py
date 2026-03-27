import logging

logger = logging.getLogger(__name__)

def run_task(name, func, *args, **kwargs):
    context = None
    
    for arg in args:
        if hasattr(arg, "symbol") and hasattr(arg, "batch_id"):
            context = arg
            break
        
    prefix = ""
    if context:
        prefix = f"[symbol={context.symbol}][batch={context.batch_id}][task={name}] "
    else:
        prefix = f"[task={name}]"
            
    logger.debug(f"{prefix}START")
    
    try:
        result = func(*args, **kwargs)
        logger.debug(f"{prefix}DONE")
        return result
    
    except Exception as e:
        logger.info(f"{prefix}FAILED: {str(e)}")
        raise
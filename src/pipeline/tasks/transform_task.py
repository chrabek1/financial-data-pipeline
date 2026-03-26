from transform.transform import transform_symbol

def transform_task(path, symbol):
    
    df = transform_symbol(path, symbol)
    
    return df
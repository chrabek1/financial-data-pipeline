from transform.transform import transform_symbol
from quality.schema import validate_schema
from quality.checks import validate_prices


def transform_task(path):
    df = transform_symbol(path)
    
    validate_schema(df)
    validate_prices(df)

    
    return df
import pandas as pd
from io import StringIO


def copy_to_staging(cur, df: pd.DataFrame):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    
    cur.copy_expert(
        """
        COPY staging_stock_daily (
        batch_id,
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        daily_return,
        rolling_avg_7,
        volatility_7
        )
        FROM STDIN WITH CSV
        """,
        buffer,
    )

def cleanup_staging(cur, batch_id: str):
    cur.execute(
        """
        DELETE FROM staging_stock_daily
        WHERE batch_id = %s
        """,
        (batch_id,),
    )

def insert_staging(cur, df, batch_id: str):
    cleanup_staging(cur, batch_id)
    copy_to_staging(cur, df)
import pandas as pd
import psycopg2
import logging
import uuid
from io import StringIO
from utils import DB_CONFIG



logger = logging.getLogger(__name__)

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def load_symbol(symbol: str, df: pd.DataFrame) -> None:
    logger.info("Starting load for %s", symbol)
    
    batch_id = str(uuid.uuid4())
    logger.info("Generated batch_id %s", batch_id)
    
    df_copy=df.copy()
    df_copy["batch_id"] = str(batch_id)
    df_copy["symbol"] = symbol
    
    df_copy = df_copy[[
        "batch_id",
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "daily_return",
        "rolling_avg_7",
        "volatility_7"
    ]]
    
    conn = get_connection()
    
    try:
        buffer = StringIO()
        df_copy.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        
        with conn.cursor() as cur:
            cur.copy_expert(
                """
                COPY staging_stock_daily (
                    batch_id, symbol, date, open, high, low, close,
                    volume, daily_return, rolling_avg_7, volatility_7
                )
                FROM STDIN WITH CSV
                """,
                buffer
            )
            cur.execute(
                """
                INSERT INTO dim_stock (symbol)
                SELECT DISTINCT symbol
                FROM staging_stock_daily
                where batch_id = %s
                ON CONFLICT (symbol) DO NOTHING
                """,
                (batch_id,)
            )
            cur.execute(
                """
                INSERT INTO dim_date (full_date, year, month, day)
                SELECT DISTINCT
                    date,
                    EXTRACT(YEAR FROM date)::int,
                    EXTRACT(MONTH FROM date)::int,
                    EXTRACT(DAY FROM date)::int
                FROM staging_stock_daily
                WHERE batch_id = %s
                ON CONFLICT (full_date) DO NOTHING;
                """,
                (batch_id,)
            )
            cur.execute(
                """
                INSERT INTO fact_stock_daily (
                    stock_id, date_id,
                    open, high, low, close,
                    volume, daily_return, 
                    rolling_avg_7, volatility_7
                )
                SELECT
                    s.stock_id,
                    d.date_id,
                    st.open,
                    st.high,
                    st.low,
                    st.close,
                    st.volume,
                    st.daily_return,
                    st.rolling_avg_7,
                    st.volatility_7
                FROM staging_stock_daily st
                JOIN dim_stock s ON st.symbol = s.symbol
                JOIN dim_date d ON st.date = d.full_date
                WHERE st.batch_id = %s
                ON CONFLICT (stock_id, date_id) DO NOTHING;
                """,
                (batch_id, )
            )
            cur.execute(
                """
                DELETE FROM staging_stock_daily WHERE batch_id = %s
                """,
                (batch_id, )
            )
            conn.commit()
            logger.info("Load completed for %s (batch_id=%s)", symbol, batch_id)
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed for {symbol}: {e}")
        raise
    
    finally:
        conn.close()
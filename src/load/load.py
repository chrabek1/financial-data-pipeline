import pandas as pd
import psycopg2
import logging
from datetime import timedelta
from io import StringIO
from utils.db import get_connection



logger = logging.getLogger(__name__)

def get_max_date_for_symbol(cur, symbol: str) -> pd.Timestamp | None:

    cur.execute(
        """
        SELECT MAX(d.full_date)
        FROM fact_stock_daily f
        JOIN dim_stock s ON f.stock_id = s.stock_id
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE s.symbol = %s
        """,
        (symbol, )
    )
    result = cur.fetchone()[0]
    return pd.to_datetime(result) if result else None

def load_symbol(cur, symbol: str, df: pd.DataFrame, batch_id: str) -> int:
    logger.info("Starting load for %s (batch_id=%s)", symbol, batch_id)
    
    df_copy=df.copy()
    
    max_date = get_max_date_for_symbol(cur, symbol)
    
    if max_date:
        refresh_from = max_date - timedelta(days=7)
        logger.info(
            "Window refresh from %s for %s",
            refresh_from.date(),
            symbol
        )
        df_copy = df_copy[df_copy["date"] >= refresh_from]
        logger.info("Filtered to %d new rows", len(df_copy))
    else:
        refresh_from = None
        
    if df_copy.empty:
        logger.info("No new data to load for %s", symbol)
        return 0
    
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
    buffer = StringIO()
    df_copy.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    if refresh_from:
        cur.execute(
                """
                DELETE FROM fact_stock_daily f
                USING dim_stock s, dim_date d
                WHERE f.stock_id = s.stock_id
                AND f.date_id = d.date_id
                AND s.symbol = %s
                AND d.full_date >= %s
                """,
                (symbol, refresh_from)
            )
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
        SELECT DISTINCT st.symbol
        FROM staging_stock_daily st
        LEFT JOIN dim_stock ds
            ON st.symbol = ds.symbol
        WHERE st.batch_id = %s
        AND ds.symbol IS NULL
        ON CONFLICT (symbol) DO NOTHING
        """,
        (batch_id,)
    )
    cur.execute(
        """
        INSERT INTO dim_date (full_date, year, month, day)
        SELECT DISTINCT
            st.date,
            EXTRACT(YEAR FROM st.date)::int,
            EXTRACT(MONTH FROM st.date)::int,
            EXTRACT(DAY FROM st.date)::int
        FROM staging_stock_daily st
        LEFT JOIN dim_date dd
            ON st.date = dd.full_date
        WHERE st.batch_id = %s
        AND dd.full_date IS NULL
        ON CONFLICT (full_date) DO NOTHING
        """,
        (batch_id,)
    )
    cur.execute(
        """
        INSERT INTO fact_stock_daily (
            stock_id, date_id,
            open, high, low, close,
            volume, daily_return, 
            rolling_avg_7, volatility_7,
            batch_id
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
            st.volatility_7,
            st.batch_id
        FROM staging_stock_daily st
        JOIN dim_stock s ON st.symbol = s.symbol
        JOIN dim_date d ON st.date = d.full_date
        WHERE st.batch_id = %s
        ON CONFLICT (stock_id, date_id) DO NOTHING;
        """,
        (batch_id, )
    )
    rows_loaded = cur.rowcount
    cur.execute(
        """
        DELETE FROM staging_stock_daily WHERE batch_id = %s
        """,
        (batch_id, )
    )
    logger.info("Load completed for %s (batch_id=%s)", symbol, batch_id)

    return rows_loaded
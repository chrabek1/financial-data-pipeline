def load_fact_table(cur, batch_id: str) -> int:
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
        (batch_id,),
    )
    
    return cur.rowcount
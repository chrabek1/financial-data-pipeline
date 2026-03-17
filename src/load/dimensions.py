def load_dimensions(cur, batch_id: str):
    # DIM STOCK
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
        (batch_id,),
    )
    
    # DIM DATE
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
        (batch_id,),
    )
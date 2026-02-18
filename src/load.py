import pandas as pd
import psycopg2
import math
from psycopg2.extras import execute_batch
from pathlib import Path

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "finance_db",
    "user": "finance_user",
    "password": "finance_pass"
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def load_csv(symbol: str):
    project_root = Path(__file__).resolve().parent.parent
    file_path = project_root / "data" / "processed" / f"{symbol}_processed.csv"
    df = pd.read_csv(file_path)
    df["date"] = pd.to_datetime(df["date"])
    return df

def get_or_create_stock(conn, symbol: str):
    with conn.cursor() as cur:
        cur.execute("SELECT stock_id FROM dim_stock WHERE symbol = %s;", (symbol,))
        result = cur.fetchone()
        
        if result:
            return result[0]
    
    cur.execute(
        "INSERT INTO dim_stock (symbol) VALUES (%s) RETURNING stock_id;",
        (symbol,)
    )
    stock_id = cur.fetchone()[0]
    conn.commit()
    return stock_id

def insert_dates(conn, df):
    with conn.cursor() as cur:
        for date in df["date"].unique():
            cur.execute(
                """
                INSERT INTO dim_date (full_date, year, month, day)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (full_date) DO NOTHING;
                """,
                (date.date(), date.year, date.month, date.day)
            )
        conn.commit()

def get_date_ids(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT date_id, full_date FROM dim_date;")
        return {row[1]: row[0] for row in cur.fetchall()}

def clean_value(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value
def insert_facts(conn, df, stock_id, date_map):
    records = []
    for _, row in df.iterrows():
        date_id = date_map[row["date"].date()]
        records.append((
        stock_id,
        date_id,
        clean_value(row["open"]),
        clean_value(row["high"]),
        clean_value(row["low"]),
        clean_value(row["close"]),
        clean_value(row["volume"]),
        clean_value(row["daily_return"]),
        clean_value(row["rolling_avg_7"]),
        clean_value(row["volatility_7"])
))

    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO fact_stock_daily (
                stock_id, date_id, open, high, low, close,
                volume, daily_return, rolling_avg_7, volatility_7
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stock_id, date_id) DO NOTHING;
        """, records)
        
        conn.commit()

if __name__ == "__main__":
    symbol = "AAPL"
    
    df = load_csv(symbol)
    conn = get_connection()
    
    stock_id = get_or_create_stock(conn, symbol)
    insert_dates(conn, df)
    
    date_map = get_date_ids(conn)
    insert_facts(conn, df, stock_id, date_map)
    
    conn.close()
    print("Data loaded successfully!")
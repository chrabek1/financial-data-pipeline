CREATE TABLE IF NOT EXISTS dim_stock (
    stock_id SERIAL PRIMARY KEY,
    symbol TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS staging_stock_daily (
    symbol TEXT,
    date DATE,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    daily_return NUMERIC,
    rolling_avg_7 NUMERIC,
    volatility_7 NUMERIC,
    batch_id TEXT
);

CREATE TABLE IF NOT EXISTS fact_stock_daily (
    stock_id INT REFERENCES dim_stock(stock_id),
    date_id INT REFERENCES dim_date(date_id),
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    daily_return NUMERIC,
    rolling_avg_7 NUMERIC,
    volatility_7 NUMERIC,
    batch_id TEXT,
    PRIMARY KEY (stock_id, date_id)
);
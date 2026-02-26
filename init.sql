DROP TABLE IF EXISTS fact_stock_daily CASCADE;
DROP TABLE IF EXISTS staging_stock_daily CASCADE;
DROP TABLE IF EXISTS dim_stock CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_stock (
    stock_id SERIAL PRIMARY KEY,
    symbol TEXT UNIQUE NOT NULL
);

CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL
);

CREATE TABLE staging_stock_daily (
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

CREATE TABLE fact_stock_daily (
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
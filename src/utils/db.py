from psycopg2.pool import ThreadedConnectionPool
from config.settings import (
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    DB_HOST,
    DB_PORT,
    DB_POOL_MIN,
    DB_POOL_MAX
)

pool = ThreadedConnectionPool(
    minconn=DB_POOL_MIN,
    maxconn=DB_POOL_MAX,
    host=DB_HOST,
    port=DB_PORT,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)

def get_connection():
    return pool.getconn()

def release_connection(conn):
    pool.putconn(conn)
import psycopg

from src.config.settings import (
    FOOTBALL_DB_HOST,
    FOOTBALL_DB_PORT,
    FOOTBALL_DB_NAME,
    FOOTBALL_DB_USER,
    FOOTBALL_DB_PASSWORD,
)


def get_db_connection():
    return psycopg.connect(
        host=FOOTBALL_DB_HOST,
        port=FOOTBALL_DB_PORT,
        dbname=FOOTBALL_DB_NAME,
        user=FOOTBALL_DB_USER,
        password=FOOTBALL_DB_PASSWORD,
    )
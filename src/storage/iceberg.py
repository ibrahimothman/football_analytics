from pathlib import Path
from pyiceberg.catalog import load_catalog
from src.config.settings import (
    FOOTBALL_DB_USER, 
    FOOTBALL_DB_PASSWORD, 
    FOOTBALL_DB_HOST, 
    FOOTBALL_DB_PORT, 
    FOOTBALL_DB_NAME,
    ICEBERG_WAREHOUSE
)

def get_catalog():
    catalog = load_catalog(
        "football_catalog",
        **{
            "type": "sql",

            "uri": (
                "postgresql+psycopg2://"
                f"{FOOTBALL_DB_USER}:{FOOTBALL_DB_PASSWORD}"
                f"@{FOOTBALL_DB_HOST}:{FOOTBALL_DB_PORT}/{FOOTBALL_DB_NAME}"
            ),

            "warehouse": (
                ICEBERG_WAREHOUSE.resolve().as_uri()
            ),
        },
    )

    catalog.create_namespace_if_not_exists("football")
    return catalog

def load_table(*, table_name: str, schema=None):
    catalog = get_catalog()
    if catalog.table_exists(table_name):
        return catalog.load_table(table_name)
    if schema is None:
        raise ValueError("Schema is required")
    return catalog.create_table(table_name, schema=schema)
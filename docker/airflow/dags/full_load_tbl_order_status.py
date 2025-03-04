import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from full_load_postgres import full_load_postgres
from load_config import load_config

def full_load_tbl_order_status():
    df_order_status = pd.read_csv("df_order_status.csv")

    config = load_config("config.yaml")
    DB_USER = config.get("DB_USER")
    DB_PASSWORD = config.get("DB_PASSWORD")
    DB_HOST = config.get("DB_HOST")
    DB_PORT = config.get("DB_PORT")
    DB_NAME = config.get("DB_NAME")
    SCHEMA_NAME = config.get("SCHEMA_NAME")
    TABLE_NAME  = "tbl_order_status"

    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
    id INT PRIMARY KEY,
    name varchar,
    created_time date
    );
    """
    df = df_order_status

    full_load_postgres(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, SCHEMA_NAME, TABLE_NAME, engine, create_table_query, df)
    



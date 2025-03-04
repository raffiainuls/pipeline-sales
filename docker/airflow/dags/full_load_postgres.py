import pandas as pd 
import psycopg2
from sqlalchemy import create_engine, text 
from load_config import load_config


config = load_config("config.yaml")
DB_USER = config.get("DB_USER")
DB_PASSWORD = config.get("DB_PASSWORD")
DB_HOST = config.get("DB_HOST")
DB_PORT = config.get("DB_PORT")
DB_NAME = config.get("DB_NAME")
SCHEMA_NAME = config.get("SCHEMA_NAME")


# creatate connection to PostgreSQL

engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')


def full_load_postgres(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, SCHEMA_NAME, TABLE_NAME, engine, create_table_query, df):
    print(f"usernamenya pakai apa nih: {DB_USER}")
    
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"
    cur.execute(create_schema_query)

    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists='append', index=False)




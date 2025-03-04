import pandas as pd 
import psycopg2 
import random 
from sqlalchemy import create_engine
from datetime import datetime, timedelta 
import os
#database configuration 

# DB_CONFIG = {
#     "user": "postgres",
#     "password": "postgres",
#     "host": "postgres",
#     "port": "5432",
#     "dbname": "sales_project",
#     "schema": "public",
#     "table": "sum_transactions"
# }



DB_CONFIG = {
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "sales_project"),
    "schema": "public",
    "table": "sum_transactions"
}

def create_db_engine(config):
    return create_engine(f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}")

def fetch_data(engine, query):
    return pd.read_sql(query, engine)

def insert_transactions(engine, transactions_df, config):
    transactions_df.to_sql(config["table"], engine, schema=config["schema"], if_exists="append", index=False)

def generate_salary_transactions(engine, config):
    query = f"""
        SELECT id, branch_id, name, salary 
        FROM {config['schema']}.tbl_employee 
        WHERE active = 'true';
    """
    df_employee = fetch_data(engine, query)
    current_year = datetime.today().year
    transactions = [
        {
            "type": "outcome",
            "sales_id": None,
            "branch_id": row["branch_id"],
            "employee_id": row["id"],
            "description": f"Monthly Salary {row['name']}",
            "date": datetime(current_year, month, 1).strftime("%Y-%m-%d"),
            "amount": row["salary"]
        }
        for _, row in df_employee.iterrows()
        for month in range(1, 13)
    ]
    insert_transactions(engine, pd.DataFrame(transactions), config)
    print("Salary transactions inserted.")

def generate_operational_expenses(engine, config, start_date, end_date):
    query = f"SELECT id, name FROM {config['schema']}.tbl_branch;"
    df_branch = fetch_data(engine, query)
    current_date = start_date
    transactions = []
    
    while current_date <= end_date:
        transactions.extend([
            {
                "type": "outcome",
                "sales_id": None,
                "branch_id": row["id"],
                "employee_id": None,
                "description": f"Pengeluaran Operasional Cabang {row['name']}",
                "date": current_date.strftime("%Y-%m-%d"),
                "amount": random.randint(200000, 400000)
            }
            for _, row in df_branch.iterrows()
        ])
        current_date += timedelta(days=1)
    
    insert_transactions(engine, pd.DataFrame(transactions), config)
    print("Operational expenses transactions inserted.")

def generate_utility_expenses(engine, config, description):
    query = f"SELECT id, name FROM {config['schema']}.tbl_branch;"
    df_branch = fetch_data(engine, query)
    current_year = datetime.today().year
    
    transactions = [
        {
            "type": "outcome",
            "sales_id": None,
            "branch_id": row["id"],
            "employee_id": None,
            "description": f"{description} {row['name']}",
            "date": datetime(current_year, month, 1).strftime("%Y-%m-%d"),
            "amount": random.randint(1000000, 2000000)
        }
        for _, row in df_branch.iterrows()
        for month in range(1, 13)
    ]
    insert_transactions(engine, pd.DataFrame(transactions), config)
    print("Utility expenses transactions inserted.")

def random_date(start_date, end_date):
    """Menghasilkan tanggal acak antara start_date dan end_date"""
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

def generate_maintenance_expenses(engine, config):
    query = f"SELECT id, name FROM {config['schema']}.tbl_branch;"
    df_branch = fetch_data(engine, query)
    transactions = []

    start = datetime(2025, 1, 1)
    end = datetime(2025, 12, 31)

    for _, row in df_branch.iterrows():
        for month in range(1,4):
            amount = random.randint(3000000, 7000000)
            transactions.append({
                "type": "outcome",
                "sales_id": None,
                "branch_id": row["id"],
                "employee_id": None, 
                "description": f"Pengeluaran Pemeliharaan Cabang {row["name"]} sebesar Rp. {amount}",
                "date": random_date(start, end),
                "amount": amount
            })

    insert_transactions(engine, pd.DataFrame(transactions), config)
    print("Maintenance expenses transactions inserted.")


def main():
    engine = create_db_engine(DB_CONFIG)
    generate_salary_transactions(engine, DB_CONFIG)
    generate_operational_expenses(engine, DB_CONFIG, datetime(2025, 1, 2), datetime(2025, 12, 31))
    generate_utility_expenses(engine, DB_CONFIG,  description='Pembayaran Air dan Listrik')
    generate_utility_expenses(engine, DB_CONFIG, description='Pengeluaran Teknologi & IT')
    generate_maintenance_expenses(engine, DB_CONFIG)

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'Raffi',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='dag_pipeline_query',
    default_args=default_args,
    start_date=datetime(2025, 3, 1),
    schedule_interval='0 3,6,9,15 * * *',
    catchup=False,
    template_searchpath=['/opt/airflow/dags/sql']
)

def create_postgres_operator(task_id, table_name, sql_file):
    query = open(f'/opt/airflow/dags/sql/{sql_file}').read()
    return PostgresOperator(
        task_id=task_id,
        postgres_conn_id="sales_project_conn",
        sql=f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS
            SELECT * FROM ({query}) AS subquery WHERE FALSE;

            TRUNCATE TABLE {table_name};

            INSERT INTO {table_name}
            {query};
        """,
        dag=dag
    )


create_fact_employee = create_postgres_operator("create_fact_employee", "fact_employee", "create_fact_employee.sql")
create_fact_sales = create_postgres_operator("create_fact_sales", "fact_sales", "create_fact_sales.sql")
create_sum_transaction = create_postgres_operator("create_sum_transaction", "sum_transaction", "create_sum_transaction.sql")
create_dim_branch_performance = create_postgres_operator("create_dim_branch_performance", "dim_branch_performance", "create_dim_branch_performance.sql")
create_dim_product_performance = create_postgres_operator("create_dim_product_performance", "dim_product_performance", "create_dim_product_performance.sql")
create_dim_monthly_branch_performance = create_postgres_operator("create_dim_monthly_branch_performance", "dim_monthly_branch_performance", "create_dim_monthly_branch_performance.sql")
create_dim_monthly_product_performance = create_postgres_operator("create_dim_monthly_product_performance", "dim_monthly_product_performance", "create_dim_monthly_product_performance.sql")
create_dim_monthly_customers_retention = create_postgres_operator("create_dim_monthly_customers_retention", "dim_monthly_customers_retention", "create_dim_monthly_customers_retention.sql")
create_dim_finance_performance = create_postgres_operator("create_dim_finance_performance", "dim_finance_performance", "create_dim_finance_performance.sql")
create_dim_monthly_finance_performance = create_postgres_operator("create_dim_monthly_finance_performance", "dim_monthly_finance_performance", "create_dim_monthly_finance_performance.sql")
create_dim_weakly_finance_performance = create_postgres_operator("create_dim_weakly_finance_performance", "dim_weakly_finance_performance", "create_dim_weakly_finance_performance.sql")
create_dim_daily_finance_performance = create_postgres_operator("create_dim_daily_finance_performance", "dim_daily_finance_performance", "create_dim_daily_finance_performance.sql")
create_dim_branch_finance_performance = create_postgres_operator("create_dim_branch_finance_performance", "dim_branch_finance_performance", "create_dim_branch_finance_performance.sql")
create_dim_branch_monthly_finance_performance = create_postgres_operator("create_dim_branch_monthly_finance_performance", "dim_branch_monthly_finance_performance", "create_dim_branch_monthly_finance_performance.sql")
create_dim_branch_weakly_finance_performance = create_postgres_operator("create_dim_branch_weakly_finance_performance", "dim_branch_weakly_finance_performance", "create_dim_branch_weakly_finance_performance.sql")
create_dim_branch_daily_finance_performance = create_postgres_operator("create_dim_branch_daily_finance_performance", "dim_branch_daily_finance_performance", "create_dim_branch_daily_finance_performance.sql")

create_fact_employee >> create_fact_sales >> create_sum_transaction >> create_dim_branch_performance >> create_dim_product_performance >> create_dim_monthly_branch_performance >> create_dim_monthly_product_performance >> create_dim_monthly_customers_retention >> create_dim_finance_performance >> create_dim_monthly_finance_performance >> create_dim_weakly_finance_performance >> create_dim_daily_finance_performance >> create_dim_branch_finance_performance >> create_dim_branch_monthly_finance_performance >> create_dim_branch_weakly_finance_performance >> create_dim_branch_daily_finance_performance

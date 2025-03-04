from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from generate_data import generate_dummy_data

default_args = {
    'owner': 'Raffi',
    'depends_on_past': False, 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id = 'dag_generate_data',
    default_args=default_args,
    start_date= datetime(2025,3,1),
    description='Generate Data Postgres',
    schedule_interval=None, 
    catchup=False,
    tags=['Project', 'Engineering'],
)

generate_data_task = PythonOperator(
    task_id = 'generate_data',
    python_callable=generate_dummy_data,
    dag=dag,
)

generate_data_task

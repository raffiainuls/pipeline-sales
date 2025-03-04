from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from insert_load_sum_transactions import main 

default_args = {
    'Owner': 'Raffi',
    'depends_on_past':False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id = 'dag_insert_load_sum_transaction',
    default_args=default_args,
    description = 'Insert Data sum_transaction',
    schedulle_interval=None, 
    catchup=False, 
    tags=['Project', 'Engineering'],
)

insert_load_sum_transaction_task = PythonOperator(
    task_id = 'insert_load_sum_transaction',
    python_callable=main,
    dag=dag,
)

insert_load_sum_transaction_task
from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from full_load_tbl_branch import full_load_tbl_branch
from full_load_tbl_customers import full_load_tbl_customers
from full_load_tbl_employee import full_load_tbl_employee
from full_load_tbl_order_status import full_load_tbl_order_status
from full_load_tbl_payment_method import full_load_tbl_payment_method
from full_load_tbl_payment_status import full_load_tbl_payment_status
from full_load_tbl_product import full_load_tbl_product
from full_load_tbl_promotions import full_load_tbl_promotions
from full_load_tbl_sales import full_load_tbl_sales
from full_load_tbl_schedulle_employee import full_load_tbl_schedulle_employee
from full_load_tbl_shipping_status import full_load_tbl_shipping_status

default_args = {
    'owner': 'Raffi',
    'depends_on_past':False,
    'retries':1, 
    'retry_delay':timedelta(minutes=5),
}

dag = DAG(
    dag_id = 'dag_full_load',
    default_args=default_args,
    start_date= datetime(2025,3,1),
    schedule_interval=None, 
    catchup=False,
    tags=['Project', 'Engineering'],
)

full_load_tbl_branch_task = PythonOperator(
    task_id = 'full_load_tbl_branch',
    python_callable=full_load_tbl_branch,
    dag=dag,
)

full_load_tbl_customers_task = PythonOperator(
    task_id = 'full_load_tbl_customers',
    python_callable=full_load_tbl_customers,
    dag=dag,
)

full_load_tbl_employee_task = PythonOperator(
    task_id = 'full_load_tbl_employee',
    python_callable=full_load_tbl_employee,
    dag=dag,
)

full_load_tbl_order_status_task = PythonOperator(
    task_id = 'full_loadd_tbl_order_status',
    python_callable=full_load_tbl_order_status,
    dag=dag,
)

full_load_tbl_payment_method_task = PythonOperator(
    task_id = 'full_load_tbl_payment_method',
    python_callable=full_load_tbl_payment_method,
    dag=dag,
)

full_load_tbl_payment_status_task = PythonOperator(
    task_id = 'full_load_tbl_payment_status',
    python_callable=full_load_tbl_payment_status,
    dag=dag,
)

full_load_tbl_product_task = PythonOperator(
    task_id = 'full_load_tbl_product',
    python_callable=full_load_tbl_product,
    dag=dag,
)

full_load_tbl_promotions_task = PythonOperator(
    task_id = 'full_load_tbl_promotions',
    python_callable=full_load_tbl_promotions,
    dag=dag,
)

full_load_tbl_sales_task = PythonOperator(
    task_id = 'full_load_tbl_sales',
    python_callable=full_load_tbl_sales,
    dag=dag,
)

full_load_tbl_schedulle_employee_task = PythonOperator(
    task_id = 'full_load_tbl_schedulle_employee',
    python_callable=full_load_tbl_schedulle_employee,
    dag=dag,
)

full_load_tbl_shipping_status_task = PythonOperator(
    task_id = 'full_load_tbl_shipping_status',
    python_callable=full_load_tbl_shipping_status,
    dag=dag
)

full_load_tbl_branch_task >> full_load_tbl_customers_task >> full_load_tbl_employee_task >> full_load_tbl_order_status_task >> full_load_tbl_payment_method_task >> full_load_tbl_payment_status_task >> full_load_tbl_product_task >> full_load_tbl_promotions_task >> full_load_tbl_sales_task >> full_load_tbl_schedulle_employee_task >> full_load_tbl_shipping_status_task

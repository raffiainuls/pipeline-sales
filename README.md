## Pipeline Sales

#### Overview
Pipeline Sales is a data pipeline project that leverages kafka for real-time data streaming and Apache Airflow for orchestrating batch and scheduled workflows. the project generates synthetic sales data, processes it in PostgreSQL, and there is full-load batch processing and real-time streaming.

#### Features 
- Data Generation: Python script generates table sales data and saves it as csv.
- Batch Processing: Airflow DAGs execute data ingestion and transaformation workflows.
- Real-time Streaming: Python script (```stream.py```) continously streams sales data to kafka, and then kafka insert into postgres with JDBC sink connector
- Scheduled Queries: Airflow DAG executes predifined queries at scheduled intervals.
- PostgreSQL Integration: All processed data is stored in a PostgresSQL database.

#### Technologies Used
- Kafka (for real-time streaming)
- Apache Airflow (for pipeline orchestration)
- Python (for data generation and streaming)
- PostgreSQL (for data storage)
- Docker (for containerized deployment)

#### Project Workflow

1. Data Generation
   - A Python script generates sales main data table such as: ```tbl_order_status, tbl_payment_method, tb_payment_status, tbl_shipping_status, tbl_employee, tbl_promotions, tbl_sales, tbl_product, tbl_schedulle_emp, tbl_customers, and tbl_branch``` and save each table into csv files
   - Executed once via an Airflow DAG
2. Full Load to PostgresSQL
   - after generate data and save into csv file there is another Airflow DAG that loads generated data into PostgreSQL. the DAG execute Python script that will read each csv file from each table, and then insert insert into PostgreSQL
   - This DAG is executed once after data generation
3. Real-time Streaming
   - The stream.py scipt continuosly sends sales data (```tbl_sales```) to kafka.
   - Kafka forwards the data to PostgreSQL for real-time processing.


<pre> ``` PipelineSales/docker/
   |-- airflow/                         # Airflow dictionary and volumes mapping docker
       |-- dags/                        # DAGs dictionary
           |-- sql/                     # dictionary for file sql 
               |-- create_dim_branch_daily_finance_performance.sql
               |-- create_dim_branch_finance_performance.sql
               |-- create_dim_branch_monthly_finance_performance.sql
               |-- create_dim_branch_performance.sql
               |-- create_dim_brannch_weakly_finance_performance.sql
               |-- create_dim_daily_finance_performance.sql
               |-- create_dim_finance_performance.sql
               |-- create_dim_monthly_branch_performance.sql
               |-- create_dim_monthly_customers_retention.sql
               |-- create_dim_monthly_finance_performance.sql
               |-- create_dim_monthly_product_performance.sql
               |-- create_dim_product_performance.sql
               |-- create_dim_weakly_finance_performance.sql
               |-- create_fact_employee.sql
               |-- create_fact_sales.sql
               |-- create_sum_transaction.sql
           |-- dag_full_load.py                     # dag for full load (execute task full load for each table)
           |-- dag_insert_load_sum_transaction.py   # dag additions for insert additional data for table sum_transactions   
           |-- dag_pipeline_generate_data.py        # dag for generate data (execute task to generate data for each table)
           |-- dag_pipeline_query.py                # dag for schedule execute query 
           |-- full_load_postgres.py                # python script function for help insert to postgres
           |-- full_load_tbl_branch.py              # python script for task full load tbl_branch
           |-- full_load_tbl_customers.py           # python script for task full load tbl_customers
           |-- full_load_tbl_employee.py            # python script for task full load tbl_employee
           |-- full_load_tbl_order_status.py        # python script for task full load tbl_order_status
           |-- full_load_tbl_payment_method.py      # python script for task full load tbl_payment_method
           |-- full_load_tbl_payment_status.py      # python script for task full load tbl_payment_status
           |-- full_load_tbl_product.py             # python script for task full load tbl_product
           |-- full_load_tbl_promotions.py          # python script for task full load tbl_promotions
           |-- full_load_tbl_sales.py               # python script for task full load tbl_sales
           |-- full_load_tbl_tbl_schedulle_employee.py # python script for task full load tbl_schedulle_employee
           |-- full_load_tbl_tbl_shipping_status.py    # python script for task full load tbl_shipping_status
           |-- generate_data.py                        # python script for task for generate data
           |-- insert_load_sum_transactions.py         # python script for task insert table load_sum_transactions
           |-- load_config.py                          # python script function for help load config.yaml
       |-- logs/                                       # logs for airflow
       |-- airflow.cfg                                 # file configuration airflow
       |-- config.yaml                                 # configurations for postgres
   |-- postgres-db-volume/              # Volume Mapping for container postgres
   |-- docker-compose.yaml              # file docker-compose 
   |-- dockerfile                       # dockerfile for container airflow
   |-- requirements.txt                 # file requirements for library python in airflow
   |-- stream_data.py                   # Python script looping data stream
 </pre>




#p
1. Clone repo:
   ```bash
   git clone https://github.com/user/repo.git

## Pipeline Sales

#### Overview
![image](https://github.com/user-attachments/assets/1cfe9fb7-9ce9-43cf-a93e-53edb6518c58)

Pipeline Sales is a data pipeline project that leverages kafka for real-time data streaming and Apache Airflow for orchestrating batch and scheduled workflows. the project generates synthetic sales data, processes it in PostgreSQL, and there is full-load batch processing and real-time streaming.

#### Features 
- Data Generation: Python script generates table sales data and saves it as csv.
- Batch Processing: Airflow DAGs execute data ingestion and transaformation workflows.
- Real-time Streaming: Python script (```stream.py```) continously streams sales data to kafka, and then kafka insert into postgres with JDBC sink connector
- Scheduled Queries: Airflow DAG executes predifined queries at scheduled intervals.
- PostgreSQL Integration: All processed data is stored in a PostgresSQL database.

### Technologies Used
- Kafka (for real-time streaming)
- Apache Airflow (for pipeline orchestration)
- Python (for data generation and streaming)
- PostgreSQL (for data storage)
- Docker (for containerized deployment)

### Project Workflow

1. Data Generation
   - A Python script generates sales main data table such as: ```tbl_order_status, tbl_payment_method, tb_payment_status, tbl_shipping_status, tbl_employee, tbl_promotions, tbl_sales, tbl_product, tbl_schedulle_emp, tbl_customers, and tbl_branch``` and save each table into csv files
   - Executed once via an Airflow DAG
2. Full Load to PostgresSQL
   ![image](https://github.com/user-attachments/assets/8067a214-0674-4d66-b6dc-b77f26ee0bea)
   - after generate data and save into csv file there is another Airflow DAG that loads generated data into PostgreSQL. the DAG execute Python script that will read each csv file from each table, and then insert insert into PostgreSQL
   - This DAG is executed once after data generation
3. Real-time Streaming

   ![image](https://github.com/user-attachments/assets/8a96f6d0-01fb-4a42-93ff-8ab4e0b2e1af)
   - The stream.py scipt continuosly sends sales data (```tbl_sales```) to kafka.
   - Kafka forwards the data to PostgreSQL for real-time processing.
4. DAGs schedulle execution query

   ![image](https://github.com/user-attachments/assets/7e3178ca-edba-440c-9f78-76326ef72b1e)
   - airflow will execute queries that create any table
   - the DAGs will be schedulle every day
   - with this DAGs query any table in postgres will be updated 

### Project Structure
<pre>  PipelineSales/docker/
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

 ### Installation & Setup 
 #### Prerequisites 
 - Docker & Docker Compose
 - Images Docker Kafka, zookeeper, debezium, Apache Airflow, PostgreSQL
 - Python

### Steps 
1. Clone the repository
   ```bash
   https://github.com/raffiainuls/pipeline-sales
   cd pipeline-sales
   cd docker
2. Start all container (```postgres, zookeeper, kafka, kafka connect, confluent control-center, airflow,```)
   ```bash
   docker-compose up
3. Wait until the all container alredy, after the all container alredy up, go to webserver airflow and then trigger the dag generate_data
4. after the dag generate_data succeed running trigger the dag full_load
5. after that trigger the dag insert_load_sum_transactions, this dag for load data tbl_sum_transactions
6. and then trigger the dag pipeline_query, this dag will run tasks that run queries to create or insert data from several tables.
7. after that run stream_data.py for streaming data to kafka, if needed adjust any configuration in stream_data.py
8. in this we alredy send data into kafka and then we need to insert data from kafka into postgres 
9. in this case we use JDBC sink connector to insert data stream into postgres 
10. the connector is alredy availabe in the repository, the file name is sink-postgres.json, we just need to ost the connector on our kafka connect, use curl for post the connector or we can also use postman to post the connector
```bash
curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d @postgres-sink.json
```
11. At this point, streaming data should be running and the resulting stream data should be in the postgres database tbl_sales

### Database Tables Overview


| Table Name      | Description |
|----------------|-------------|
| `tbl_sales`        |main sales table, contains sales transaction data. The data source for this table is from full load, and streaming data  
| `tbl_branch`     | this table contains informations about all branch that active and not active. the data source for this table is from full load |
| `tbl_customers`    | this table contains informations about all coustomers. The source for this table is from full load |
| `tbl_employee`      | this table contains informations about all employee who still active or not. The source for this table is from full load |
| `tbl_order_status`    | master data of order status. The source for this table is from full load |
| `tbl_payment_method`       | master data of payment method. The source for this table is from full load |
| `tbl_payment_status`       | master data of payment status. The source for this table is from full load |
| `tbl_product`       | master data of product. The source for this table is from full load |
| `tbl_promotions`       | master data of tabel promotions. The source for this table is from full load |
| `tbl_schedulle_employee`       | this table contains informations about schedulle employee. the source for this table is from full load |
| `tbl_shipping_status`       | master data of shipping status. The source for this table is from full load |
| `tbl_dim_branch_daily_finance_performance`       | this table contains information daily finance. this table is formed from dags queries that airflow runs on a scheduled basis every day |
| `tbl_dim_branch_finance_performance`       | this table contains finance performance for each branch. This table is formed from dags queries that airflow runs on a scheduled basis every day |
| `tbl_dim_branch_monthly_finance_performance`       | this table contains monthly finance performance for each branch. This table is formed from dags queries that airflow runs on a schduled basis every day |
| `tbl_dim_branch_performance`       | this table contains branch performance. This table is formed from dags queries that airflow runs on a schduled basis every day |
| `tbl_dim_branch_weakly_finance_performance`       |this table contains weakly finance performance for each branch. This table is formed from dags queries that airflow runs on a schduled basis every day |
| `tbl_dim_daily_finance_performance`       | this table contains daily finance performance. This table is formed from dags queries that airflow runs on a schduled basis every day  |
| `tbl_dim_finance_performance`       | this table contains finance performance. This table is formed from dags queries that airflow runs on a schduled basis every day |
| `tbl_dim_monthly_branch_performance`       | this table contains monthly performance for each branch. This table is formed from dags queries that airflow runs on a schduled basis every day |
| `tbl_dim_monthly_customers_retentiton`       | this table contains information about customer retention. This table is formed from dags queries that airflow runs on a schduled basis every day |
| `tbl_dim_monthly_finance_performance`       | this table contains monthly finance performance. This table is formed from dags queries that airflow runs on a schduled basis every day |
| `tbl_dim_monthly_product_performance`       | this table contains monthly product performance. This table is formed from dags queries that airflow runs on a schduled basis every day |
| `dim_product_performance`       | this table contains product performance. This table is formed from dags queries that airflow runs on a schduled basis every day |
| `dim_weakly_finance_performance`       | this table contains weakly finance performance. This table is formed from dags queries that airflow runs on a schduled basis every day |
| `fact_employee`       | this table contains information about employee who still active. This table is formed from dags queries that airflow runs on a schduled basis every day |
| `sum_transaction`       | this table contains about information summary income and outcome data. the datasourcer for this table is form full load and update data from dags queries that airflow runs on a schduled basis every day |








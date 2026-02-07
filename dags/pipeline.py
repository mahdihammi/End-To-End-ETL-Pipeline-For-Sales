from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import logging
from airflow.providers.standard.operators.python import PythonOperator

from include.meddallion_architecture.bronze_layer import raw_orders, bronze_table
from include.meddallion_architecture.silver_layer import silver_table
from include.meddallion_architecture.gold_layer import create_gold_table


LOCAL_DUCKDB_CONN_ID = "my_local_duckdb_conn"
LOCAL_DUCKDB_TABLE_NAME = "orders_bronze"
SILVER_TABLE_NAME = "orders_silver"

GOLD_SCHEMA_NAME = "gold"
SILVER_SCHEMA_NAME = "silver"


GOLD_TABLES = [
    "gold_sales_by_time",
    "gold_product_performance",
    "gold_customer_segments",
]


@dag(
    dag_id="lakehouse_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "duckdb"],     
)
def data_lakehouse_dag():
    @task
    def start_dag_logger():
        logging.info("Starting DuckDB Environment Variable DAG")

    sensor_minio_bucket_and_upload = PythonOperator(
        task_id="sensor_minio_bucket_and_upload",
        python_callable=raw_orders,
    )

    create_bronze_table = PythonOperator(
        task_id="create_bronze_layer",
        python_callable=bronze_table,
        op_kwargs={
            "LOCAL_DUCKDB_CONN_ID": LOCAL_DUCKDB_CONN_ID,
            "LOCAL_DUCKDB_TABLE_NAME": LOCAL_DUCKDB_TABLE_NAME,
            "RAW_BUCKET": "raw",
        },
    )

    create_silver_table = PythonOperator(
        task_id="create_silver_layer",
        python_callable=silver_table,
        op_kwargs={
            "LOCAL_DUCKDB_CONN_ID": LOCAL_DUCKDB_CONN_ID,
            "BRONZE_TABLE_NAME": LOCAL_DUCKDB_TABLE_NAME,
            "SILVER_TABLE_NAME": SILVER_TABLE_NAME,
        },
    )

    # Create gold tables sequentially in a task group
    with TaskGroup("gold_layer_creation") as gold_group:
        previous_task = None
        for table_name in GOLD_TABLES:
            gold_task = PythonOperator(
                task_id=table_name,
                python_callable=create_gold_table,
                op_kwargs={
                    "LOCAL_DUCKDB_CONN_ID": LOCAL_DUCKDB_CONN_ID,
                    "SILVER_TABLE_NAME": "orders_silver",
                    "table_name": table_name,
                    "GOLD_SCHEMA_NAME": GOLD_SCHEMA_NAME,
                    "SILVER_SCHEMA_NAME": SILVER_SCHEMA_NAME,
                },
            )
            if previous_task:
                previous_task >> gold_task
            previous_task = gold_task

    @task
    def verify_gold_layer():
        print("✅ All gold tables created successfully!")

    start_dag_logger() >> sensor_minio_bucket_and_upload >> create_bronze_table >> create_silver_table >> gold_group >> verify_gold_layer()


data_lakehouse_dag()
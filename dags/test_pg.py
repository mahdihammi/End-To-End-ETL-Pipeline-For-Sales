from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime
from include.medall_arch.bronze_layer import test_db_query

@dag(
    dag_id="test_pg",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
)

def dag_pg():

    @task
    def dummy_task():
        print("starting task")

    query_pg = PythonOperator(
        task_id="test_pg",
        python_callable = test_db_query
    )

    dummy_task() >> query_pg

dag_pg()


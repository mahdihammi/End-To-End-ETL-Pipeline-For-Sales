import os
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime

from include.medall_arch.views import creating_views





LOCAL_DUCKDB_CONN_ID = os.environ.get('LOCAL_DUCKDB_CONN_ID')



@dag(
    dag_id="view_dag_test",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
)


def dag_pg():

    create_layer_views = creating_views(LOCAL_DUCKDB_CONN_ID)

    create_views = PythonOperator(
        task_id = "create_views",
        python_callable = create_layer_views
    )

    create_views

dag_pg()








from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import DAG

default_args = {
    'owner': 'siva',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
     'start_date': datetime(2021, 11, 1)
}

dag = DAG(

    dag_id = 'dag_with_catch_backfll3',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup=False
)

def printstrings():
    print("hello this is siva")


t1 = PythonOperator(
    task_id="priya",
    python_callable= printstrings,
    dag=dag
)
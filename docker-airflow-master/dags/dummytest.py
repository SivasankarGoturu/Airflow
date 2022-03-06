from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

default_args={
    'owner' : 'Airflow',
    'start_date' : datetime(2021, 12, 5),
    'retries' : 1,
    'retry_delay' : timedelta(seconds=5)
}

dag = DAG(
        "dummy",
        default_args=default_args,
        schedule_interval = "@daily",
        catchup=False)

def printStrings():
    print("Hello this is siva")

t1 = PythonOperator(task_id="printStrings", python_callable=printStrings, dag=dag)
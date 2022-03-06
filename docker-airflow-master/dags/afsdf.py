from warnings import catch_warnings

from airflow import DAG
# from airflow.utils.timezone import datetime
from datetime import datetime, timedelta

from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'dev_siva',
    'start_date': datetime(2022, 2, 6),
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False
}

dag = DAG(
    dag_id='Customers_Data_Pipeline',
    default_args=default_args,
    catchup=False
    #schedule_interval='*/1 * * * *'
)

t1 = BashOperator(
    task_id='ddsd',
    bash_command='echo sivasankar',
    dag=dag
)

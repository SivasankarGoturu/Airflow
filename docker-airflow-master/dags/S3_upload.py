from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.bash_operator  import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils import apply_defaults


default_args = {
    'owner' : 'sivasankar',
    'email' : ['sivasankaronly@gmail.com'],
    'email_on_failure' : True,
    #'retries' : 1,
    'start_date' : datetime(2021, 12, 9),
    'retry_delay' : timedelta(seconds= 10)

}

dag = DAG(

    dag_id= "dataPipeLine",
    default_args= default_args,
    schedule_interval= '@daily',
    catchup= False
)

class MyOperator(BaseOperator):
    @apply_defaults
    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

    def execute(self, context):
        message= 'hello {}'.format(self.name)
        print(message)
        return message

def mys3():
    print("Hello this is siva s3 data pipeline")
"""
t1 = PythonOperator(
    task_id= "pullingDataFromS3",
    dag=dag,
    python_callable= mys3

)
"""

t1 = BashOperator(
    task_id= "pullingDataFromS3",
    dag=dag,
    bash_command= '{{ var.value.mycommand }}'
)


t2 = BashOperator(
    task_id="printing_the_date",
    bash_command='date',
    dag=dag
)

t3 = BashOperator(
    task_id='printing_my_name',
    bash_command='echo hello sivasankar',
    dag=dag
)

t4 = BashOperator(
    task_id='printing_something',
    bash_command='echo hello siva sankar,sadnfkjnaskjdfhkjhaskjdfhkjhsadkjfhkjhasdkjlhfuohasdojfhakjsdhfij',
    dag=dag,
    trigger_rule= 'all_failed'
)

t5 = MyOperator(
    name = 'My name is sivasankar',
    task_id='userdefinedOperator',
    dag=dag,
    trigger_rule='one_success'

)

sensor = HttpSensor(

    task_id='checkingURL',
    endpoint='/',
    start_date = datetime(2021, 12, 8),
    http_conn_id='myconection',
    retries=20,
    retry_delay=timedelta(seconds=1)


)

#t1 >> t2 >> t3

sensor >> t1 >> [t4,t2,t3] >> t5
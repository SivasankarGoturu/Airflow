from airflow import DAG
# from airflow.utils.timezone import datetime
from datetime import datetime, timedelta

from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'dev_siva',
    'start_date': datetime(2022, 2, 6),
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False
}

dag = DAG(
    dag_id='Customers_data_pipeline5',
    default_args=default_args
    # schedule_interval='0 * * * * *',
)


def extract_customers_data():
    sqoop_command = """
    
    sqoop-import-all-tables \
    -Dhadoop.security.credential.provider.path=jceks://hdfs/user/itv001703/mysql.password.jceks \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password-alias  mysql.sivadev.password \
    --num-mappers 5 \
    --as-parquetfile \
    --autoreset-to-one-mapper \
    --warehouse-dir rawdata
    
    """
    return f'{sqoop_command}'


sqooping =SSHOperator(
    task_id='sqooping_data',
    ssh_conn_id='itversity',
    command=extract_customers_data(),
    dag=dag
)


mybash = BashOperator(
    task_id='justbash',
    bash_command='echo sqoopCompleted',
    dag=dag

)

sqooping >> mybash
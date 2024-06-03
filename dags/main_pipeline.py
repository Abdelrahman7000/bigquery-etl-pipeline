from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Abdelrahman',
    'retries': 10,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False
}
# This will run every morning at 9
dag= DAG(
    dag_id='my_dag',
    default_args=default_args,
    start_date=datetime(2024,6, 3),
    schedule_interval='0 9 * * *' 
) 



database_build = BashOperator(
    task_id='Database_build',
    bash_command='python airflow_pro/dags/script/db_build/build_db.py',
    dag=dag
)
    
Extract = BashOperator(
    task_id='Extract',
    bash_command='python airflow_pro/dags/script/Extract.py',
    dag=dag
)
    
Transform_Load = BashOperator(
    task_id='Transform_and_Load',
    bash_command='python airflow_pro/dags/script/Transform_and_Load.py',
    dag=dag
)
    
database_build >> Extract >> Transform_Load
    
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

test_dag = DAG(
    dag_id = 'hello_dag',
    start_date = datetime(2022,5,5),
    catchup=False,
    tags=['example'],
    schedule_interval = '0 2 * * *')

t1 = BashOperator(
   task_id='print_date',
   bash_command='date',
   dag=test_dag)
t2 = BashOperator(
   task_id='sleep',
   bash_command='sleep 5',
   retries=3,
   dag=test_dag)
t3 = BashOperator(
   task_id='ls',
   bash_command='ls /tmp',
   dag=test_dag)
   
t1 >> t2
t1 >> t3

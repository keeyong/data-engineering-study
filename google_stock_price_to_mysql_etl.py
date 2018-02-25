"""
 A sample Airflow job copying doing the followings:
 - copying Google Stock price info from Google Finance
 - pushing to a local MySQL

 You need to have this table created in your local MySQL

 create table test.google_stock_price (
    date date NOT NULL PRIMARY KEY,
    open float,
    high float,
    low float,
    close float,
    volume int
 );
"""
from datetime import timedelta
from datetime import datetime

import airflow
import pymysql
import csv

from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# note that there is no schedule_interval
# which means the only way to run this job is:
# 1> manual trigger in the Airflow web interface
# 2> programmatic trigger from other job
dag = DAG(
    'google_stock_price',
    default_args=default_args
)

dag.doc_md = __doc__

# note the followings:
# 1> it is using a http connection named "google_finance" which is created separately
#    from Airflow web interface (menu:Admin -> Connections)
# 2> xcom_push is set to True so that the downloaded content is pushed to Xcom which
#    will be retrieved in the next Operator instance
t1 = SimpleHttpOperator(
    task_id='get_google_stock',
    http_conn_id='google_finance',
    method='GET',
    endpoint='finance/historical?q=goog&startdate=01-Jan-2010&output=csv',
    xcom_push=True,
    dag=dag
)

def pull_csv_and_push_to_mysql(**kwargs):
    """
    A callback function used in PythonOperator instance.
    - Pull the google stock price info from Xcom
    - Push those records into MySQL
    """
    # Another tip for debugging is to print something and check logs folder in $AIRFLOW_HOME
    # print(kwargs)
    value = kwargs['task_instance'].xcom_pull(task_ids='get_google_stock')

    reader = csv.reader(value.split("\n"))
    # skip the header
    # Date,Open,High,Low,Close,Volume
    next(reader)

    conn = pymysql.connect(host='localhost',
                       user='root', password='keeyonghan',
                       db='test', charset='utf8')
    curs = conn.cursor()
    for row in reader:
        sql = "insert into test.google_stock_price value ('{date}', {open}, {high}, {low}, {close}, {volume});".format(
            date=datetime.strptime(row[0], "%d-%b-%y").date(),
            open=row[1],
            high=row[2],
            low=row[3],
            close=row[4],
            volume=row[5] if row[5] != '-' else None
        )
        print(sql)
        curs.execute(sql)
    conn.close()
    '''
    - For debugging purpose, you can write to a file
    f = open("keeyong.csv", "w")
    f.write(value)
    f.close()
    '''

t2 = PythonOperator(
    task_id='read_csv',
    provide_context=True,
    dag=dag,
    python_callable=pull_csv_and_push_to_mysql)

t1 >> t2

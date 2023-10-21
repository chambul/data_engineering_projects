from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from etl import run_etl # import function

default_args = {
    'owner': 'airflow', # keep track of who wrote the dag
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

#create the dag
dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description="Twitter ETL pipeline"
)

run__task_etl = PythonOperator(
    task_id = 'complete_twitter_etl',
    python_callable=run_etl, # python function to call
    dag=dag,
)

run__task_etl
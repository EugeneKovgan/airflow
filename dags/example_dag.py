from airflow import DAG
from airflow.operators.empty import EmptyOperator  # type: ignore
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('example_dag', default_args=default_args, schedule_interval='@daily')

start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

start >> end

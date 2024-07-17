# dags/tiktok_followers.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from common.common_functions import fetch_tiktok_followers_data, save_data_to_mongo
from typing import Any, Dict

def save_followers_data(**kwargs: Dict[str, Any]) -> None:
    data = fetch_tiktok_followers_data()
    if data:
        save_data_to_mongo('tiktok_followers_test', data, kwargs['ts'])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'tiktok_followers',
    default_args=default_args,
    description='Fetch TikTok followers stats and save to MongoDB',
    schedule_interval=None,
    start_date=days_ago(1),
)

tiktok_followers_task = PythonOperator(
    task_id='tiktok_followers',
    python_callable=save_followers_data,
    provide_context=True,
    dag=dag,
)

tiktok_followers_task

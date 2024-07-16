from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient
import os
from tikapi import TikAPI

def fetch_tiktok_followers_data():
    api_key = os.getenv("TIKAPI_KEY")
    auth_key = os.getenv("TIKAPI_AUTHKEY")
    api = TikAPI(api_key)
    user = api.user(accountKey=auth_key)
    try:
        data = user.analytics(type="followers")
        return data.json()
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        raise

def save_followers_data(**kwargs):
    mongo_url = os.getenv("MONGO_URL")
    mongo_dbname = os.getenv("MONGO_DBNAME")
    client = MongoClient(mongo_url)
    db = client[mongo_dbname]
    
    data = fetch_tiktok_followers_data()
    if data:
        db.tiktok_followers_test.insert_one({
            "data": data,
            "recordCreated": kwargs['ts']
        })
    client.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'tiktok_followers_stats',
    default_args=default_args,
    description='Fetch TikTok followers stats and save to MongoDB',
    schedule_interval=None,
    start_date=days_ago(1),
)

tiktok_followers_task = PythonOperator(
    task_id='tiktok_followers_stats',
    python_callable=save_followers_data,
    provide_context=True,
    dag=dag,
)

tiktok_followers_task

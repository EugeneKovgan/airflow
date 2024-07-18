from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from common.common_functions import save_parser_history, get_mongo_client, get_tikapi_client
from typing import Any, Dict
import pendulum

def fetch_tiktok_followers_data() -> Dict[str, Any]:
    user = get_tikapi_client()
    try:
        data = user.analytics(type="followers")
        return data.json()
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        raise

def save_data_to_mongo(collection_name: str, data: Dict[str, Any], ts: str) -> None:
    db = get_mongo_client()
    db[collection_name].insert_one({
        "data": data,
        "recordCreated": ts
    })

def save_followers_data(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'TikTok Followers Data'
    status = 'success'
    start_time = pendulum.now()
    total_followers = 0

    try:
        db = get_mongo_client()
        data = fetch_tiktok_followers_data()
        ts = kwargs['ts']  
        if not isinstance(ts, str):
            raise ValueError("Timestamp 'ts' must be a string")
        if data:
            save_data_to_mongo('tiktok_followers', data, ts)
            total_followers = len(data)  

    except Exception as error:
        status = 'failure'
        print(f"Error during processing: {str(error)}")
        raise
    finally:
        save_parser_history(db, parser_name, start_time, 'followers', total_followers, status)

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

# dags/combine_videos.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import UpdateOne
from common.common_functions import get_mongo_client, combine_videos_object
from typing import Any, Dict

def extract_and_combine(**kwargs: Dict[str, Any]) -> None:
    db = get_mongo_client()

    tiktok_posts = list(db.tiktok_posts.find())
    instagram_reels = list(db.instagram_reels.find())
    youtube_videos = list(db.youtube_videos.find())

    combined_data = []

    for post in tiktok_posts:
        transformed_post = combine_videos_object(post, "tiktok")
        combined_data.append(transformed_post)
    
    for reel in instagram_reels:
        transformed_reel = combine_videos_object(reel, "instagram")
        combined_data.append(transformed_reel)
    
    for video in youtube_videos:
        transformed_video = combine_videos_object(video, "youtube")
        combined_data.append(transformed_video)

    operations = []
    for document in combined_data:
        print(f"Document to upsert: {document}")
        operations.append(UpdateOne(
            {'_id': document['_id']},
            {'$set': document},
            upsert=True
        ))

    if operations:
        db.combined_videos.bulk_write(operations, ordered=False)

    print("Data combined and saved to new collection in remote MongoDB")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'combine_videos',
    default_args=default_args,
    description='A DAG to combine three collections from remote MongoDB with platform field and save to a new collection in the same remote MongoDB',
    schedule_interval=None,
    start_date=days_ago(1),
)

extract_and_combine_task = PythonOperator(
    task_id='extract_and_combine',
    python_callable=extract_and_combine,
    dag=dag,
)

extract_and_combine_task

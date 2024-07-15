from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient, InsertOne, UpdateOne
import os

def extract_and_combine(**kwargs):
    remote_mongo_uri = os.getenv("REMOTE_MONGO_URI")
    remote_mongo_db = os.getenv("REMOTE_MONGO_DB")

    print(f"REMOTE_MONGO_URI: {remote_mongo_uri}")
    print(f"REMOTE_MONGO_DB: {remote_mongo_db}")

    # Подключение к удаленной MongoDB
    remote_client = MongoClient(remote_mongo_uri)
    remote_db = remote_client[remote_mongo_db]

    # Извлечение данных из коллекций с добавлением поля platform
    tiktok_posts = list(remote_db.tiktok_posts.find())
    for post in tiktok_posts:
        post['platform'] = 'tiktok'

    instagram_reels = list(remote_db.instagram_reels.find())
    for reel in instagram_reels:
        reel['platform'] = 'instagram'

    youtube_videos = list(remote_db.youtube_videos.find())
    for video in youtube_videos:
        video['platform'] = 'youtube'

    # Объединение данных
    combined_data = tiktok_posts + instagram_reels + youtube_videos

    # Подготовка операций для пакетного выполнения
    operations = []
    for document in combined_data:
        operations.append(UpdateOne(
            {'_id': document['_id']},
            {'$set': document},
            upsert=True
        ))

    # Выполнение пакетной операции
    if operations:
        remote_db.combined_videos.bulk_write(operations, ordered=False)

    print("Data combined and saved to new collection in remote MongoDB")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'combine_three_collections_to_new_collection_with_platform_dag',
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

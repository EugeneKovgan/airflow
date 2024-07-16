from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient, UpdateOne
import os

def transform_to_unified_schema(document, platform):
    video = document.get("video", {})
    if platform == "tiktok":
        stats = video.get("stats", {})
        return {
            "_id": document.get("_id"),
            "recordCreated": document.get("recordCreated"),
            "tags": document.get("tags"),
            "platform": platform,
            "video": {
                "author": video.get("author", {}).get("nickname"),
                "created_at": video.get("createTime"),
                "description": video.get("desc"),
                "id": video.get("id"),
                'duration': video.get("music", {}).get("duration"),
                "video_url": video.get("video", {}).get("cover"),
                "stats": {
                    "collectCount": stats.get("collectCount"),
                    "commentCount": stats.get("commentCount"),
                    "diggCount": stats.get("diggCount"),
                    "playCount": stats.get("playCount"),
                    "shareCount": stats.get("shareCount"),
                    "followers_count": None,
                }
            }
        }
    elif platform == "instagram":
        return {
            "_id": document.get("_id"),
            "recordCreated": document.get("recordCreated"),
            "tags": document.get("tags"),
            "platform": platform,
            "video": {
                "author": video.get("author_name"),  
                "created_at": video.get("createTime"),
                "description": video.get("desc"),
                "id": video.get("id"),
                'duration': None,
                "video_url": video.get("cover"),
                "stats": {
                    "collectCount": None,
                    "commentCount": video.get("comment_count"),
                    "diggCount": video.get("like_count"),
                    "playCount": None,
                    "shareCount": None,
                    "followers_count":  video.get("profile_picture_url", {}).get("followers_count"),
                }
            }
        }
    elif platform == "youtube":
        return {
            "_id": document.get("_id"),
            "recordCreated": document.get("recordCreated"),
            "tags": document.get("tags"),
            "platform": platform,
            "video": {
                "author": video.get("author"),
                "created_at": video.get("createTime"),
                "description": video.get("desc"),
                "id": video.get("id"),
                'duration': None,
                "video_url": video.get("url"),
                "stats": {
                    "collectCount": None,
                    "commentCount": None,
                    "diggCount": None,
                    "playCount": None,
                    "shareCount": None,
                    "followers_count": None,
                }
            }
        }
    else:
        return {
            "_id": document.get("_id"),
            "recordCreated": document.get("recordCreated"),
            "tags": document.get("tags"),
            "platform": platform,
            "video": {
                "author": None,
                "created_at": None,
                "description": None,
                "id": video.get("id"),
                'duration': None,
                "video_url": None,
                "stats": {
                    "collectCount": None,
                    "commentCount": None,
                    "diggCount": None,
                    "playCount": None,
                    "shareCount": None,
                    "followers_count": None,
                }
            }
        }

def extract_and_combine(**kwargs):
    mongo_uri = os.getenv("MONGO_URL")
    mongo_db = os.getenv("MONGO_DBNAME")

    print(f"MONGO_URL: {mongo_uri}")
    print(f"MONGO_DBNAME: {mongo_db}")

    remote_client = MongoClient(mongo_uri)
    remote_db = remote_client[mongo_db]

    tiktok_posts = list(remote_db.tiktok_posts.find())
    instagram_reels = list(remote_db.instagram_reels.find())
    youtube_videos = list(remote_db.youtube_videos.find())

    combined_data = []
    for post in tiktok_posts:
        transformed_post = transform_to_unified_schema(post, "tiktok")
        combined_data.append(transformed_post)
    
    for reel in instagram_reels:
        transformed_reel = transform_to_unified_schema(reel, "instagram")
        combined_data.append(transformed_reel)
    
    for video in youtube_videos:
        transformed_video = transform_to_unified_schema(video, "youtube")
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
        remote_db.combined_videos.bulk_write(operations, ordered=False)

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

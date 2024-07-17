# common/common_functions.py

from datetime import datetime
from pymongo import MongoClient
import os
import pendulum
from tikapi import TikAPI
from typing import Any, Dict
from dataclasses import asdict
from common.schemas import Document, Video, VideoStats

def get_mongo_client() -> MongoClient:
    mongo_url = os.getenv("MONGO_URL")
    mongo_dbname = os.getenv("MONGO_DBNAME")
    client = MongoClient(mongo_url)
    db = client[mongo_dbname]
    return db

def get_tikapi_client() -> TikAPI:
    api_key = os.getenv("TIKAPI_KEY")
    auth_key = os.getenv("TIKAPI_AUTHKEY")
    api = TikAPI(api_key)
    return api.user(accountKey=auth_key)

# def fetch_tiktok_followers_data() -> Dict[str, Any]:
#     user = get_tikapi_client()
#     try:
#         data = user.analytics(type="followers")
#         return data.json()
#     except Exception as e:
#         print(f"Error fetching data: {str(e)}")
#         raise

def save_data_to_mongo(collection_name: str, data: Dict[str, Any], ts: str) -> None:
    db = get_mongo_client()
    db[collection_name].insert_one({
        "data": data,
        "recordCreated": ts
    })
    
def parse_datetime(datetime_str):
    if isinstance(datetime_str, str):
        return pendulum.parse(datetime_str)
    elif isinstance(datetime_str, datetime):
        return pendulum.instance(datetime_str)
    return None

def save_parser_history(db, parser_name, start_time, data_type, total_count, status):
    db.parser_history_test.insert_one({
        "parser_name": parser_name,
        "start_time": start_time,
        "end_time": datetime.utcnow(),
        "data_type": data_type,
        "total_count": total_count,
        "status": status
    })

def combine_videos_object(document: Dict[str, Any], platform: str) -> Dict[str, Any]:
    video = document.get("video", {})
    stats = video.get("stats", {})
    return asdict(Document(
        _id=document.get("_id"),
        recordCreated=document.get("recordCreated"),
        tags=document.get("tags"),
        platform=platform,
        video=Video(
            author=video.get("author", {}).get("nickname") if platform == "tiktok" else video.get("author_name") if platform == "instagram" else video.get("author") if platform == "youtube" else None,
            createTime=video.get("createTime") if platform in ["tiktok", "youtube", "instagram"] else None,
            description=video.get("desc") if platform in ["tiktok", "youtube", "instagram"] else None,
            id=video.get("id"),
            duration=video.get("music", {}).get("duration") if platform == "tiktok" else None,
            video_url=video.get("video", {}).get("playAddr") if platform == "tiktok" else video.get("cover") if platform == "instagram" else video.get("url") if platform == "youtube" else None,
            img_url=video.get("video", {}).get("cover") if platform == "tiktok" else video.get("image_url") if platform == "instagram" else video.get("cover") if platform == "youtube" else None,
            stats=VideoStats(
                collectCount=stats.get("collectCount") if platform == "tiktok" else None,
                commentCount=stats.get("commentCount") if platform == "tiktok" else video.get("comment_count") if platform == "instagram" else None,
                diggCount=stats.get("diggCount") if platform == "tiktok" else video.get("like_count") if platform == "instagram" else None,
                playCount=stats.get("playCount") if platform == "tiktok" else None,
                shareCount=stats.get("shareCount") if platform == "tiktok" else None,
                followers_count=None if platform == "tiktok" else video.get("profile_picture_url", {}).get("followers_count") if platform == "instagram" else None,
            )
        )
    ))

from pymongo import MongoClient
import os
from tikapi import TikAPI

def get_mongo_client():
    mongo_url = os.getenv("MONGO_URL")
    mongo_dbname = os.getenv("MONGO_DBNAME")
    client = MongoClient(mongo_url)
    db = client[mongo_dbname]
    return db

def get_tikapi_client():
    api_key = os.getenv("TIKAPI_KEY")
    auth_key = os.getenv("TIKAPI_AUTHKEY")
    api = TikAPI(api_key)
    return api.user(accountKey=auth_key)

def fetch_tiktok_followers_data():
    user = get_tikapi_client()
    try:
        data = user.analytics(type="followers")
        return data.json()
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        raise

def save_data_to_mongo(collection_name, data, ts):
    db = get_mongo_client()
    db[collection_name].insert_one({
        "data": data,
        "recordCreated": ts
    })

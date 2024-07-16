# core/schemas.py

unified_schema = {
    "_id": None,
    "author": None,
    "comments": None,
    "created_at": None,
    "description": None,
    "id": None,
    "likes": None,
    "video_url": None
}

tiktok_mapping = {
    "_id": "_id",
    "author": "author",
    "comments": "comments",
    "created_at": "createTime",
    "description": "desc",
    "id": "id",
    "likes": "stats.diggCount",
    "video_url": "video.downloadAddr"
}

instagram_mapping = {
    "_id": "_id",
    "author": "profile.username",
    "comments": "comments_count",
    "created_at": "createTime",
    "description": "desc",
    "id": "id",
    "likes": "likes_count",
    "video_url": "video_url"
}

youtube_mapping = {
    "_id": "_id",
    "author": "author",
    "comments": "comments_count",
    "created_at": "created_at",
    "description": "description",
    "id": "video_id",
    "likes": "likes_count",
    "video_url": "video_url"
}

def map_fields(document, mapping):
    # Add debugging print statements
    print(f"Mapping document: {document}")
    mapped_document = {unified_field: document.get(mapping_field, None) for unified_field, mapping_field in mapping.items()}
    print(f"Mapped document: {mapped_document}")
    return mapped_document

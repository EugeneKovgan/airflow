from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum
from common.common_functions import close_mongo_connection, get_mongo_client, save_parser_history, handle_parser_error, log_parser_start, log_parser_finish, get_tikapi_client
from typing import Any, Dict

def get_tiktok_comments(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Tiktok Comments'
    status = 'success'
    proceed = True
    start_time = pendulum.now()
    log_parser_start(parser_name)

    db = get_mongo_client()
    total_comments_count = 0

    try:
        user = get_tikapi_client()
        posts_collection = db['tiktok_posts']
        comments_collection = db['tiktok_comments']

        video_ids = fetch_video_ids(posts_collection)
        
        for video_id in video_ids:
            if not proceed:
                break
            try:
                total_comments_count += process_video_comments(user, comments_collection, video_id, parser_name)
            except Exception as e:
                result = handle_parser_error(e, parser_name, proceed)
                proceed = result["proceed"]
                status = result["status"]
                if not proceed:
                    break
                print(f"{parser_name}: Error processing comments for video {video_id}: {e}")

    except Exception as error:
        result = handle_parser_error(error, parser_name, proceed)
        status = result["status"]
        proceed = result["proceed"]
        print(f"{parser_name}: Ошибка: {error}")
    finally:
        if db:
            save_parser_history(db, parser_name, start_time, 'comments', total_comments_count, status)
        close_mongo_connection(db.client)
        log_parser_finish(parser_name)

def fetch_video_ids(posts_collection) -> list:
    return posts_collection.distinct('video.id')

def process_video_comments(user, comments_collection, video_id, parser_name) -> int:
    total_comments_count = 0
    retry_attempts = 3

    while retry_attempts > 0:
        try:
            response = user.posts.comments.list(media_id=video_id)
            comments = response.json().get('comments', [])
            for comment in comments:
                total_comments_count += process_comment(user, comments_collection, comment)
            break
        except Exception as error:
            retry_attempts -= 1
            if retry_attempts == 0:
                raise error
            result = handle_parser_error(error, parser_name, True)
            print(f"Retrying... {3 - retry_attempts} attempts left.")
    
    return total_comments_count

def process_comment(user, comments_collection, comment) -> int:
    existing_comment = comments_collection.find_one({'data.cid': comment['cid']})

    if not existing_comment:
        reply_comments = fetch_reply_comments(user, comment) if comment['reply_comment_total'] > 0 else []
        comments_collection.insert_one({
            'data': comment,
            'recordCreated': pendulum.now(),
            'reply': reply_comments,
        })
        return 1
    else:
        if existing_comment['data']['reply_comment_total'] != comment['reply_comment_total']:
            updated_reply_comments = fetch_reply_comments(user, comment) if comment['reply_comment_total'] > 0 else []
            comments_collection.replace_one(
                {'data.cid': comment['cid']},
                {
                    'data': comment,
                    'recordCreated': pendulum.now(),
                    'reply': updated_reply_comments,
                }
            )
        else:
            print(f"Skipping update for comment with cid {comment['cid']}. reply_comment_total has not changed.")
        return 0

def fetch_reply_comments(user, comment) -> list:
    response = user.posts.comments.replies(media_id=comment['aweme_id'], comment_id=comment['cid'])
    
    print(f"Fetched reply comments for comment {comment['cid']}: {response.json().get('comments', [])}")
    return response.json().get('comments', [])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'tiktok_comments',
    default_args=default_args,
    description='Fetch TikTok comments and save to MongoDB',
    schedule_interval=None,
    start_date=days_ago(1),
)

tiktok_comments_task = PythonOperator(
    task_id='tiktok_comments',
    python_callable=get_tiktok_comments,
    provide_context=True,
    dag=dag,
)

tiktok_comments_task

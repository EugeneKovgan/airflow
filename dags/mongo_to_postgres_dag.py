from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('mongo_to_postgres', default_args=default_args, schedule_interval='@daily')

def transfer_data():
    # Подключение к MongoDB
    mongo_client = MongoClient('mongodb://ekovgan:BqTa3vljNREo6sd4@mongo02.izdat.com:27018/')
    mongo_db = mongo_client.get_default_database()
    mongo_collection = mongo_db.settings

    # Подключение к PostgreSQL
    pg_conn = psycopg2.connect(
        dbname="your_postgres_db",
        user="your_postgres_user",
        password="your_postgres_password",
        host="your_postgres_host",
        port="5432"
    )
    pg_cursor = pg_conn.cursor()

    # Получение данных из MongoDB
    mongo_data = mongo_collection.find()

    # Перенос данных в PostgreSQL
    for document in mongo_data:
        columns = document.keys()
        values = [document[column] for column in columns]
        insert_statement = f"INSERT INTO your_postgres_table ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))})"
        pg_cursor.execute(insert_statement, values)
    
    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()
    mongo_client.close()

transfer_data_task = PythonOperator(
    task_id='transfer_data_task',
    python_callable=transfer_data,
    dag=dag,
)

transfer_data_task

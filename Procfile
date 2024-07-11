release: airflow db init && airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
web: airflow webserver -p $PORT
scheduler: airflow scheduler

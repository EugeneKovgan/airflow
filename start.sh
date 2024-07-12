#!/bin/bash

# Initialize the database
airflow db init

# Create an admin user
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin

# Start the scheduler in the background
airflow scheduler &

# Start the web server
exec airflow webserver -p $PORT
FROM apache/airflow:2.6.3-python3.8

ENV AIRFLOW_HOME=/opt/airflow

# Установим зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Скопируем все необходимые файлы
COPY dags/ $AIRFLOW_HOME/dags/
# COPY core/ $AIRFLOW_HOME/core/
COPY .env $AIRFLOW_HOME/

# Установим переменную PYTHONPATH, включающую директорию core
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/core"

USER airflow

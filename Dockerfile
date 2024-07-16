FROM apache/airflow:2.6.3-python3.8

ENV AIRFLOW_HOME=/opt/airflow

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy necessary files
COPY dags/ $AIRFLOW_HOME/dags/
COPY core/ $AIRFLOW_HOME/core/
COPY .env $AIRFLOW_HOME/

# Set PYTHONPATH to include core directory
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/core"

USER airflow
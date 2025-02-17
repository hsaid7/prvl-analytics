FROM apache/airflow:2.5.1-python3.9

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:airflow requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=airflow:airflow dags/ /opt/airflow/dags/

COPY --chown=airflow:airflow entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

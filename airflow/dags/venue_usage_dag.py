from airflow import DAG
from airflow_dbt_python.operators.dbt import (
    DbtRunOperator,
    DbtTestOperator,
    DbtDepsOperator,
)
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email': ['alert@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def ingest_data():
    import subprocess
    subprocess.run(['python', '/scripts/data_ingestion.py'], check=True)

with DAG(
    'venue_usage_pipeline',
    default_args=default_args,
    description='Pipeline to process venue usage data',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 10, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=ingest_data,
    )

    run_dbt_deps = DbtDepsOperator(
        task_id='dbt_deps',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        upload_dbt_project=False,
    )

    run_dbt_models = DbtRunOperator(
        task_id='dbt_run',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        #models=['my_model'],  # Specify the models to run
    )

    run_dbt_tests = DbtTestOperator(
        task_id='dbt_test',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        #models=['my_model'],  # Specify the models to test
    )

    extract_data >> run_dbt_deps >> run_dbt_models >> run_dbt_tests
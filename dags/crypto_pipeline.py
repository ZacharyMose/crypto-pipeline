from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# ==========================================
# CONFIGURATION
# ==========================================
# UPDATE THIS to the output of 'pwd' command
PROJECT_PATH = "/home/mose/Desktop/Data-Engineering/crypto-lakehouse"

# We use the python inside the venv to ensure libraries like pandas/boto3 are found
PYTHON_EXEC = f"{PROJECT_PATH}/venv/bin/python"
DBT_EXEC = f"{PROJECT_PATH}/venv/bin/dbt"
# ==========================================

default_args = {
    'owner': 'root',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crypto_etl_pipeline',
    default_args=default_args,
    description='Fetch crypto data, load to Postgres, transform with dbt',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['crypto', 'etl'],
) as dag:

    # Task 1: Extraction (API -> S3)
    t1_extract = BashOperator(
        task_id='extract_from_api',
        # We CD into the project folder first so .env and relative paths work
        bash_command=f'cd {PROJECT_PATH} && {PYTHON_EXEC} scripts/extract.py',
        env={**os.environ.copy()}  # Pass existing env vars (like AWS keys)
    )

    # Task 2: Loading (S3 -> Postgres)
    t2_load = BashOperator(
        task_id='load_to_postgres',
        bash_command=f'cd {PROJECT_PATH} && {PYTHON_EXEC} scripts/load.py',
        env={**os.environ.copy()}
    )

    # Task 3: Transformation (dbt run)
    t3_transform = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {PROJECT_PATH}/dbt_project && {DBT_EXEC} run',
        env={**os.environ.copy()}
    )

    # Task 4: Data Quality (dbt test)
    t4_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {PROJECT_PATH}/dbt_project && {DBT_EXEC} test',
        env={**os.environ.copy()}
    )

    # Dependency Chain
    t1_extract >> t2_load >> t3_transform >> t4_test
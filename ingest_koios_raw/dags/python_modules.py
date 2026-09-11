from pendulum import datetime
from airflow.decorators import dag
from airflow import DAG
from airflow.operators.python import PythonOperator,get_current_context
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta


SCRIPTS_DIR = "/usr/local/airflow/dags/scripts/drugdevelopment/ingest_koios_raw"


with DAG(
    "python_modules",
    description="koios raw ingestion",
    start_date=datetime(2023, 6, 8),
    catchup=False,
) as dag:
    

    dbt_debug = BashOperator(
        task_id="requirements_loading",
        bash_command=f"pip install boto3; pip install --force-reinstall 'pandas<2.2.0'; pip install detect-delimiter; pip install sqlalchemy; pip install chardet; pip install fsspec; pip install s3fs",
    )


    dbt_debug

from pendulum import datetime
from airflow.decorators import dag
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.email import send_email_smtp, send_email

from airflow.operators.python import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.models import Variable
import os
from datetime import datetime, timedelta
from textwrap import dedent
import json

DBT_PROJECT_DIR = "/tmp/drugdevelopment/koios"
SCRIPTS_DIR = "~/scripts/drugdevelopment"
DBT_path = "/usr/local/airflow/.local/bin/dbt"
email = Variable.get("koios_airflow_alert_email").split(",")


def task_success_callback(context):
    print('context = ',context)
    outer_task_success_callback(context, email=email)


def outer_task_success_callback(context, email):
	subject = "[Airflow Success Alerts] DAG {0}: Success".format(
		context['task_instance_key_str'].split('__')[0],
		)
	html_content = """
	DAG: {0}<br>
	All tasks succeeded<br>
	Succeeded on: {1}
	""".format(
		context['task_instance_key_str'].split('__')[0],
		datetime.now()
		)
	send_email_smtp(email, subject, html_content)


default_args={
    "depends_on_past": False,
    "email": email,
    "email_on_success": task_success_callback,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1
}

with DAG(
    "koios_mart_datamodel",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=default_args,
    description="koios main dag in mwaa clearlake for mart datamodel",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    start = BashOperator(
        task_id="start",
        env={'DAPLEX_KOIOS_HOST': '{{ var.value.DAPLEX_KOIOS_HOST }}', 'DAPLEX_KOIOS_USER':
             '{{ var.value.DAPLEX_KOIOS_USER }}', 'DAPLEX_KOIOS_PASSWORD': '{{ var.value.DAPLEX_KOIOS_PASSWORD }}',
             'DAPLEX_KOIOS_DB': '{{ var.value.DAPLEX_KOIOS_DB }}','DAPLEX_KOIOS_SCHEMA': '{{ var.value.DAPLEX_KOIOS_SCHEMA }}'},
        bash_command="cp -R /usr/local/airflow/dags/dbt/drugdevelopment /tmp/; ls; cd /tmp/drugdevelopment; ls;"
    )

    dbt_deps = BashOperator(
        task_id="koios_deps",
        env={'DAPLEX_KOIOS_HOST': '{{ var.value.DAPLEX_KOIOS_HOST }}', 'DAPLEX_KOIOS_USER':
             '{{ var.value.DAPLEX_KOIOS_USER }}', 'DAPLEX_KOIOS_PASSWORD': '{{ var.value.DAPLEX_KOIOS_PASSWORD }}',
             'DAPLEX_KOIOS_DB': '{{ var.value.DAPLEX_KOIOS_DB }}','DAPLEX_KOIOS_SCHEMA': '{{ var.value.DAPLEX_KOIOS_SCHEMA }}'},
        bash_command=f"{DBT_path} deps --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_clean = BashOperator(
        task_id="koios_clean",
        env={'DAPLEX_KOIOS_HOST': '{{ var.value.DAPLEX_KOIOS_HOST }}', 'DAPLEX_KOIOS_USER':
             '{{ var.value.DAPLEX_KOIOS_USER }}', 'DAPLEX_KOIOS_PASSWORD': '{{ var.value.DAPLEX_KOIOS_PASSWORD }}',
             'DAPLEX_KOIOS_DB': '{{ var.value.DAPLEX_KOIOS_DB }}','DAPLEX_KOIOS_SCHEMA': '{{ var.value.DAPLEX_KOIOS_SCHEMA }}'},
        bash_command=f"{DBT_path} clean --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_stagging = BashOperator(
        task_id="koios_staging",
        env={'DAPLEX_KOIOS_HOST': '{{ var.value.DAPLEX_KOIOS_HOST }}', 'DAPLEX_KOIOS_USER':
             '{{ var.value.DAPLEX_KOIOS_USER }}', 'DAPLEX_KOIOS_PASSWORD': '{{ var.value.DAPLEX_KOIOS_PASSWORD }}',
             'DAPLEX_KOIOS_DB': '{{ var.value.DAPLEX_KOIOS_DB }}','DAPLEX_KOIOS_SCHEMA': '{{ var.value.DAPLEX_KOIOS_SCHEMA }}'},
        bash_command= f"{DBT_path} run --models staging.* --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_seed = BashOperator(
        task_id="koios_seed",
        env={'DAPLEX_KOIOS_HOST': '{{ var.value.DAPLEX_KOIOS_HOST }}', 'DAPLEX_KOIOS_USER':
             '{{ var.value.DAPLEX_KOIOS_USER }}', 'DAPLEX_KOIOS_PASSWORD': '{{ var.value.DAPLEX_KOIOS_PASSWORD }}',
             'DAPLEX_KOIOS_DB': '{{ var.value.DAPLEX_KOIOS_DB }}','DAPLEX_KOIOS_SCHEMA': '{{ var.value.DAPLEX_KOIOS_SCHEMA }}'},
        bash_command= f"{DBT_path} seed --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_mart = BashOperator(
        task_id="koios_mart",
        env={'DAPLEX_KOIOS_HOST': '{{ var.value.DAPLEX_KOIOS_HOST }}', 'DAPLEX_KOIOS_USER':
             '{{ var.value.DAPLEX_KOIOS_USER }}', 'DAPLEX_KOIOS_PASSWORD': '{{ var.value.DAPLEX_KOIOS_PASSWORD }}',
             'DAPLEX_KOIOS_DB': '{{ var.value.DAPLEX_KOIOS_DB }}','DAPLEX_KOIOS_SCHEMA': '{{ var.value.DAPLEX_KOIOS_SCHEMA }}'},
        bash_command= f"{DBT_path} run --models mart.* --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    end = BashOperator(
        task_id="end",
        bash_command=f"cd /tmp/drugdevelopment/; rm -r koios;",
    )

    start >> dbt_clean >>  dbt_deps >> dbt_stagging >> dbt_seed >> dbt_mart >> end

    

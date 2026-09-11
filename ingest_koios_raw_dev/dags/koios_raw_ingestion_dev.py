from pendulum import datetime
from airflow.decorators import dag
from airflow import DAG
from airflow.operators.python import PythonOperator,get_current_context
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email_smtp, send_email
# from airflow.providers.amazon.aws.sensors.s3 import S3KeysUnchangedSensor
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Normal call style
raw_bucket_name = Variable.get("clearlake_raw_bucket_dev")
secret_name = Variable.get("koios_db_secret_name_dev")
email = Variable.get("koios_airflow_alert_email")


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

SCRIPTS_DIR = "/usr/local/airflow/drugdevelopment/dags/scripts/drugdevelopment/ingest_koios_raw_dev"

def execute():
    context = get_current_context()
    execution_date = context.get("execution_date")
    print(execution_date)

with DAG(
    "koios_raw_ingestion_dev",
    default_args=default_args,
    schedule=None,
    description="koios raw ingestion development",
    start_date=datetime(2023, 11, 29,7,45),
    # end_date=datetime(2024,12,31),
    catchup=False,
) as dag:
    
    start = EmptyOperator( 
        task_id="start",
        trigger_rule="all_success"
    )

    raw_deletion = BashOperator( 
        task_id="koios_truncate",
        trigger_rule="all_success",
        bash_command= f'python3 {SCRIPTS_DIR}/delete_statements.py',
        dag=dag
    )


    
    raw_ingestion = BashOperator( 
        task_id="koios_raw_ingestion",
        trigger_rule="all_success",
        bash_command= f'python3 {SCRIPTS_DIR}/loop_config.py',
        dag=dag
    )

    # dbt_debug = BashOperator(
    #     task_id="requirements_loading",
    #     bash_command=f"pip install boto3; pip install pandas; pip install detect-delimiter; pip install sqlalchemy; pip install chardet;",
    # )
    # Data_model_trigger = TriggerDagRunOperator(
    # task_id="Koios_model_trigger",
    # trigger_dag_id="koios_mart_datamodel_dev"
    # )


    end = EmptyOperator( 
        task_id="end",
        trigger_rule="all_success"
    )


    # start >> raw_deletion >> raw_ingestion >> Data_model_trigger >> end
    start >> raw_deletion >> raw_ingestion >> end

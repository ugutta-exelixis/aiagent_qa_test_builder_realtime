from pendulum import datetime
from airflow.decorators import dag
from airflow import DAG
from airflow.operators.python import PythonOperator,get_current_context
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email_smtp, send_email
# from airflow.providers.amazon.aws.sensors.s3 import S3KeysUnchangedSensor
import traceback
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from airflow.models.connection import Connection
from airflow.operators.python_operator import PythonOperator

# Normal call style
raw_bucket_name = Variable.get("clearlake_raw_bucket")
secret_name = Variable.get("koios_db_secret_name")
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
    # 'on_success_callback': task_success_callback,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1
}

SCRIPTS_DIR = "/usr/local/airflow/dags/scripts/drugdevelopment/ingest_koios_manual"
# module_man = SCRIPTS_DIR+"/task_success_callback"
import sys

def task_failure_callback(context):
    print('context = ',context)
    outer_task_failure_callback(context, email=email)

def outer_task_failure_callback(context, email):
	subject = "Failure Alert [Airflow] DAG {0} - Task {1}: Failed".format(
		context['task_instance_key_str'].split('__')[0],
		context['task_instance_key_str'].split('__')[1]
		)
	html_content = """
	DAG: {0}<br>
	Task: {1}<br>
	Failed on: {2}
	""".format(
		context['task_instance_key_str'].split('__')[0],
		context['task_instance_key_str'].split('__')[1],
		datetime.now()
		)
	send_email_smtp(email, subject, html_content)



with DAG(
    "airflow_alerts_test",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=default_args,
        #"retry_delay": timedelta(minutes=1),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    #},
    description="airflow alerts testing",
    schedule= None,
    start_date=datetime(2023, 11, 29,7,45),
    end_date=datetime(2024,12,31),
    catchup=False,
) as dag:
    
    start = EmptyOperator( 
        task_id="start",
        # on_failure_callback="send_email",
        trigger_rule="all_success"
    )

    test_airflow = BashOperator(
        task_id="test_airflow",
        trigger_rule="all_success",
        on_failure_callback=task_failure_callback,
        bash_command= f'python3 {SCRIPTS_DIR}/test_airflow.py'
    )

    end = EmptyOperator( 
        task_id="end",
        trigger_rule="all_success"
    )


    start >> test_airflow >> end
    # >> test_airflow
    
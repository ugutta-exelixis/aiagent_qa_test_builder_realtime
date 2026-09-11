from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
# /usr/local/airflow/dags/scripts/drugdevelopment/ingest_koios_raw
with DAG(
   dag_id="check_dbt_path",
   schedule_interval=None,
   catchup=False,
   start_date=days_ago(1),
) as dag:
   check_dbt_path = BashOperator(
      task_id="check_dbt_path",
      bash_command="cd /usr/local/airflow/; ls;"
   )

   check_all_path = BashOperator(
      task_id="check_all_path",
      bash_command="cd /usr/local/airflow/dags/scripts/; ls;"
   )

   check_user_bin = BashOperator(
      task_id="check_user_bin",
      bash_command="cd /usr/local/airflow/dags/scripts/drugdevelopment/; ls;"
   )

   check_local = BashOperator(
      task_id="check_local",
      bash_command="cd /usr/local/airflow/dags/scripts/drugdevelopment/ingest_koios_raw; ls;"
   )

   check_local_bin = BashOperator(
      task_id="check_local_bin",
      bash_command="cd /usr/local/bin/; ls;"
   )

   check_airflow = BashOperator(
       task_id="check_airflow",
       bash_command="cd /usr/local/airflow/; ls;"
   )

   check_airflow_local = BashOperator(
       task_id="check_airflow_local",
       bash_command="cd /usr/local/airflow/.local/; ls;"
   )
   
   check_plugins = BashOperator(
       task_id="check_plugins",
       bash_command="cd /usr/local/airflow/plugins/; ls;"
   )

   find_dbt_in_venv = BashOperator(
       task_id="find_dbt_in_virtualenv",
       bash_command="""
       source /usr/local/airflow/python3-virtualenv/dbt-env/bin/activate && \
       which dbt && \
       deactivate
       """
   )

   patrick_dbt_path = BashOperator(
       task_id="patrick_dbt_path",
       bash_command="cd /usr/local/airflow/python3-virtualenv/dbt-env/bin/; ls;"
   )
check_dbt_path >> check_all_path >> check_user_bin >> check_local >> check_local_bin >> check_airflow >> check_airflow_local >> check_plugins >> find_dbt_in_venv >> patrick_dbt_path

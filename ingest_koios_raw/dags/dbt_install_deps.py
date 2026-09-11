from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

with DAG(dag_id="dbt-Install-Deps", schedule_interval=None, catchup=False, start_date=days_ago(1)) as dag:
    ls_airflow_plugins = BashOperator(
        task_id="ls_airflow_plugins",
        bash_command="ls -laR /usr/local/airflow/requirements",
        dag=dag,
        priority_weight=300,
    )
    run_upgrade_command = BashOperator(
        task_id="pip_upgrade",
        bash_command="pip3 install --upgrade pip",
        dag=dag,
        priority_weight=300,
    )
    Install_requirements = BashOperator(
        task_id="Install_requirements",
        bash_command="pip3 install -r /usr/local/airflow/requirements/requirements.txt",
        dag=dag,
        priority_weight=300,
    )

    install_dbt_core_command = BashOperator(
        task_id="Install_dbt_core",
        bash_command="pip3 install /usr/local/airflow/plugins/dbt_core-1.6.5-py3-none-any.whl",
        dag=dag,
        priority_weight=300,
    )
    install_dbt_pg_command = BashOperator(
        task_id="Install_dbt_postgres",
        bash_command="pip3 install /usr/local/airflow/plugins/dbt_postgres-1.6.5-py3-none-any.whl",
        dag=dag,
        priority_weight=300,
    )

    Install_Numpy = BashOperator(
        task_id="Install_Numpy",
        bash_command="pip3 install /usr/local/airflow/plugins/numpy-1.26.1-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
        dag=dag,
        priority_weight=300,
    )

    Install_Pandas = BashOperator(
        task_id="Install_Pandas",
        bash_command="pip3 install /usr/local/airflow/plugins/pandas-1.4.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
        dag=dag,
        priority_weight=300,
    )

    cli_command = BashOperator(
        task_id="dbt_version",
        bash_command="/usr/local/airflow/.local/bin/dbt --version"
    )

    Pip_Freeze = BashOperator(
        task_id="Pip_Freeze",
        bash_command="pip3 freeze"
    )


    ls_airflow_plugins >> run_upgrade_command >> Install_requirements >>install_dbt_core_command >>install_dbt_pg_command >> Install_Numpy >> Install_Pandas >> cli_command >> Pip_Freeze

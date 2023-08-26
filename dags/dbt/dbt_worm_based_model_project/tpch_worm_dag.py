import os
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.models import Variable


# DAG Variable
dag_var = Variable.get("tpch_worm_model_dag",deserialize_json=True, default_var=None)

dag = DAG(dag_var.get('dag_id'),
            description=dag_var.get('description'),
            schedule_interval=dag_var.get('schedule_interval'),
            start_date=datetime(2023, 6, 12), catchup=False)

# Variables
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}dags/dbt/dbt_worm_based_model_project"
DBT_CONNECTION_PATH = f"{os.environ['AIRFLOW_HOME']}dags/dbt/connection_profiles/"

# Snpashot Date Variable
DT="{{ ds }}"

# Variable to determine a snapshot action or a read action
ACTION = dag_var.get('action')

# Initialize dag components
start_dummy_operator = DummyOperator(task_id='start', dag=dag)

# short circuit operator
snapshot_action = ShortCircuitOperator(
                task_id="action_is_for_snapshot",
                python_callable=lambda: ACTION=='snapshot')

read_action = ShortCircuitOperator(
                task_id="action_is_for_read",
                python_callable=lambda: ACTION=='read')

execute_dbt_snapshotting_model = BashOperator(
                 task_id='dbt_run_snapshot_mode',
                 bash_command=f"""dbt run --profiles-dir {DBT_CONNECTION_PATH} --project-dir {DBT_PROJECT_PATH} --select tag:complete_refresh --vars '{{refresh_date: { DT } }}' """
                 )

execute_dbt_read_model = BashOperator(
                 task_id='dbt_run_read_mode',
                 bash_command=f"""dbt run --profiles-dir {DBT_CONNECTION_PATH} --project-dir {DBT_PROJECT_PATH} --select tag:complete_refresh --exclude dbt_worm_based_model_project.source.SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER_SNAPSHOT --vars '{{refresh_date: { DT } }}' """
                 )

end_dummy_operator = DummyOperator(task_id='end', dag=dag)

start_dummy_operator >> snapshot_action >> execute_dbt_snapshotting_model >> end_dummy_operator
start_dummy_operator >> read_action >> execute_dbt_read_model >> end_dummy_operator

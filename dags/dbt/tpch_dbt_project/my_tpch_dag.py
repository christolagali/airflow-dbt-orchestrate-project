import os
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

# DAG Variable
dag_var = Variable.get("tpch_first_dag",deserialize_json=True, default_var=None)

dag = DAG(dag_var.get('dag_id'),
            description=dag_var.get('description'),
            schedule_interval=dag_var.get('schedule_interval'),
            start_date=datetime(2023, 6, 12), catchup=False)

# Variables
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}dags/dbt/tpch_dbt_project"
DBT_CONNECTION_PATH = f"{os.environ['AIRFLOW_HOME']}dags/dbt/connection_profiles/"

start_dummy_operator = DummyOperator(task_id='start', dag=dag)

execute_dbt_model = BashOperator(
                    task_id='dbt_model_run',
                    bash_command=f"""dbt run --profiles-dir {DBT_CONNECTION_PATH} --project-dir {DBT_PROJECT_PATH} --select tpch_model.tpch_model""".format(DBT_PROJECT_PATH=DBT_PROJECT_PATH,DBT_CONNECTION_PATH=DBT_CONNECTION_PATH)
                    )


end_dummy_operator = DummyOperator(task_id='end', dag=dag)

start_dummy_operator>>execute_dbt_model>>end_dummy_operator

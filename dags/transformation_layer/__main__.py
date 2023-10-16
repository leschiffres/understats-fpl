from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
from airflow.hooks.base import BaseHook
from transformation_layer.team_transformation import team_form

CONNECTION_ID = "LOCAL_POSTGRES"

postgres = BaseHook.get_connection(CONNECTION_ID)

postgres_config = {
    "host":postgres.host,
    "port": postgres.port,
    "user": postgres.login,
    "password": postgres.password,
    "database": postgres.schema,
}

# Define your DAG
with DAG(
    'transformation_layer',
    start_date=datetime.datetime(2023, 10, 11),
    schedule_interval=None,
    catchup=False,
) as dag:

    team_forml_task = PythonOperator(
        task_id='team_form',  # Unique task ID
        python_callable=team_form,
        op_kwargs={'postgres_config':postgres_config}
    )

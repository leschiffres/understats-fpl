from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
from airflow.hooks.base import BaseHook
from transformation_layer.team_transformation import team_form, team_future_opponents_ratings
from transformation_layer.player_transformation import player_form

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

    team_form_task = PythonOperator(
        task_id='team_form',  # Unique task ID
        python_callable=team_form,
        op_kwargs={'postgres_config':postgres_config}
    )

    team_future_opponents_ratings_task = PythonOperator(
        task_id='team_future_opponents_ratings',  # Unique task ID
        python_callable=team_future_opponents_ratings,
        op_kwargs={'postgres_config':postgres_config}
    )

    player_form_task = PythonOperator(
        task_id='player_form',  # Unique task ID
        python_callable=player_form,
        op_kwargs={'postgres_config':postgres_config}
    )
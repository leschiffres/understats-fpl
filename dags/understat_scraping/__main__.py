from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
from airflow.hooks.base import BaseHook
from understat_scraping.player_retrieval import aggregated_player_retrieval, player_recent_matches_retrieval
from understat_scraping.team_match_data_retrieval import team_past_matches_retrieval, team_match_data_retrieval

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
    'understat_scraping',
    start_date=datetime.datetime(2023, 10, 11),
    schedule_interval=None,
    catchup=False,
) as dag:

    aggregated_player_retrieval_task = PythonOperator(
        task_id='aggregated_player_retrieval',  # Unique task ID
        python_callable=aggregated_player_retrieval,
        op_kwargs={'postgres_config':postgres_config}
    )
    
    player_recent_matches_retrieval_task = PythonOperator(
        task_id='player_recent_matches',  # Unique task ID
        python_callable=player_recent_matches_retrieval,
        op_kwargs={'postgres_config':postgres_config}
    )


    # team_past_matches_retrieval_task = PythonOperator(
    #     task_id='team_past_matches_retrieval',  # Unique task ID
    #     python_callable=team_past_matches_retrieval,
    #     op_kwargs={'postgres_config':postgres_config}
    # )

    # team_future_matches_retrieval_task = PythonOperator(
    #     task_id='team_match_data_retrieval',  # Unique task ID
    #     python_callable=team_match_data_retrieval,
    #     op_kwargs={'postgres_config':postgres_config}
    # )

aggregated_player_retrieval_task >> player_recent_matches_retrieval_task

# aggregated_player_retrieval_task >> team_match_data_task

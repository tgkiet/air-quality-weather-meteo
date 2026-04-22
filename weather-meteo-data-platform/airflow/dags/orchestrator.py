from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'gkinhere-airflow',
    'description': 'Orchestrator DAG for OpenMeteo API data pipeline',
    'retries': 3,
    'start_date': datetime(2026, 4, 22),
}

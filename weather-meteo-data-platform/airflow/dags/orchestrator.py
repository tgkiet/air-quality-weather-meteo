from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'gkinhere-airflow',
    'description': 'Orchestrator DAG for OpenMeteo API data pipeline',
    'retries': 3,
    'start_date': datetime(2026, 5, 1),
}

# 1. Khởi tạo DAG sử dụng context manager (with)
with DAG(
    dag_id='open_meteo_api_pipeline_orchestrator',
    default_args=default_args,
    schedule=timedelta(hours=1),
    catchup=False
) as dag:
    # 2. Định nghĩa các task sử dụng BashOperator
    fetch_data = BashOperator(
        task_id='fetch_data',
        bash_command='python3 /home/kiet/gkinhere/air-quality-pipeline/air-quality-weather-meteo/weather-meteo-data-platform/src/main.py'
    )
    
    fetch_data
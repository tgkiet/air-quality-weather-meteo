from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'gkinhere-airflow',
    'description': 'Orchestrator DAG for OpenMeteo API data pipeline',
    # Airflow tự động retry 3 lần nếu task FAILED
    'retries': 3,
    'start_date': datetime(2026, 5, 1),
}

# Khởi tạo DAG bằng context manager
with DAG(
    dag_id='open_meteo_api_pipeline_orchestrator',
    default_args=default_args,
    schedule='@hourly',
    # catchup=False: Không chạy bù các kỳ đã qua khi DAG lần đầu được bật.
    # Chỉnh thành True khi cần Backfill dữ liệu lịch sử.
    catchup=False,
) as dag:

    # -------------------------------------------------------------------------
    # Task: Extract & Load
    #
    # Jinja Templating được sử dụng để "tiêm" (inject) Logical Date của Airflow
    # vào file main.py thông qua tham số dòng lệnh --execution_date.
    #
    # {{ logical_date | ts }} => Chuỗi ISO 8601, ví dụ: "2026-05-06T10:00:00+00:00"
    # ĐÂY LÀ THỜI ĐIỂM AIRFLOW LẬP LỊCH ĐỂ CHẠY TASK, không phải datetime.now().
    # Dù task bị Retry lúc 10h05, biến này vẫn giữ nguyên giá trị 10h00.
    # Đây là cơ chế CỐT LÕI đảm bảo tính Lũy Đẳng (Idempotency) của Pipeline.
    # -------------------------------------------------------------------------
    fetch_data = BashOperator(
        task_id='fetch_data',
        bash_command=(
            'python3 /opt/airflow/src/main.py '
            '--execution_date "{{ logical_date | ts }}"'
        ),
    )

    fetch_data
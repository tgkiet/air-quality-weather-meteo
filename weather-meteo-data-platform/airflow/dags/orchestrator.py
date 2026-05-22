from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DEFAULT ARGS - Cấu hình mặc định áp dụng cho toàn bộ các Task trong DAG này.
# Tách riêng ra để tuân thủ nguyên tắc DRY (Don't Repeat Yourself).
default_args = {
    'owner': 'gkinhere-airflow',
    'description': 'Orchestrator DAG for OpenMeteo Weather & Air Quality ELT pipeline (20 grid-cell locations)',
    # Airflow tự động retry 3 lần nếu task FAILED, trước khi báo lỗi thật sự.
    # Quan trọng: Retry vẫn dùng cùng execution_date → Đảm bảo Idempotency.
    'retries': 3,
    # Không retry ngay lập tức — chờ 5 phút để API/DB có thể recover.
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 5, 1),
}

# DAG DEFINITION
with DAG(
    dag_id='open_meteo_api_pipeline_orchestrator',
    default_args=default_args,
    schedule='@hourly',
    # Chỉnh thành True khi cần Backfill dữ liệu lịch sử.
    catchup=False,
) as dag:

    # TASK 1: EXTRACT & LOAD (E → L)
    # Jinja Templating: {{ logical_date | ts }} inject Logical Date của Airflow
    # vào main.py qua tham số --execution_date.
    #
    # Ví dụ: "2026-05-06T10:00:00+00:00"
    # ĐÂY LÀ THỜI ĐIỂM AIRFLOW LẬP LỊCH ĐỂ CHẠY TASK, không phải datetime.now().
    # Dù task bị Retry lúc 10h05, biến này vẫn giữ nguyên giá trị 10h00.
    # Đây là cơ chế CỐT LÕI đảm bảo tính Lũy Đẳng (Idempotency) của Pipeline.
    fetch_data = BashOperator(
        task_id='fetch_data',
        bash_command=(
            'python3 /opt/airflow/src/main.py '
            '--execution_date "{{ logical_date | ts }}"'
        ),
    )

    # TASK 2: TRANSFORM — dbt run (T)
    # Cơ chế: Gọi lệnh "dbt run" trực tiếp bên trong airflow_container.
    # dbt-postgres đã được cài sẵn qua requirements_airflow.txt.
    #
    # --project-dir: Trỏ đến thư mục dbt project (được mount vào Airflow container).
    # --profiles-dir: Trỏ đến thư mục chứa profiles.yml (được mount vào ~/.dbt).
    #
    # Tại sao KHÔNG dùng docker exec?
    #   → Cần mount Docker socket (/var/run/docker.sock) — rủi ro bảo mật nghiêm trọng.
    #   → Process trong container có thể kiểm soát toàn bộ Docker daemon trên host.
    #   → Giải pháp này (dbt CLI trực tiếp) không cần socket, an toàn hơn hoàn toàn.
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            'dbt run '
            '--project-dir /opt/airflow/dbt-transform '
            '--profiles-dir /home/airflow/.dbt'
        ),
    )

    # TASK 3: DATA QUALITY GATE — dbt test (Validate)
    # Chạy SAU dbt_run. Nếu test FAILED, Airflow đánh dấu task này là FAILED.
    # Các lần chạy tiếp theo sẽ biết là có vấn đề ở data quality.
    #
    # 29 data quality tests phủ sóng 3 tầng:
    #   - Bronze : 7 tests (unique + not_null cho cả 2 bảng Bronze)
    #   - Silver : 8 tests (not_null + location_name warn cho 2 bảng Silver)
    #   - Gold   : 14 tests (not_null dimensions + accepted_values labels + alerts)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=(
            'dbt test '
            '--project-dir /opt/airflow/dbt-transform '
            '--profiles-dir /home/airflow/.dbt'
        ),
    )

    # TASK DEPENDENCIES - Thứ tự chạy
    # Đọc: fetch_data PHẢI hoàn thành → dbt_run mới được kích hoạt
    #      dbt_run  PHẢI hoàn thành → dbt_test mới được kích hoạt
    # Nếu fetch_data FAILED → dbt_run và dbt_test bị SKIP (không chạy).
    # Đảm bảo không Transform dữ liệu rác khi bước Load thất bại.
    fetch_data >> dbt_run >> dbt_test

# DESIGN NOTES
# Tại sao không tách fetch_weather và fetch_aq thành 2 Task riêng?
#   → Vì cả 2 đều gọi chung API Open-Meteo, chia sẻ cùng session HTTP.
#   → Tách ra = over-engineering + risk duplicate connection + khó maintain.
#   → Pipeline này chọn gộp 1 Task = đơn giản, đủ dùng, dễ debug.
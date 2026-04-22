# Airflow DAGs (Orchestration Layer)

Thư mục này chứa các file định nghĩa DAG (Directed Acyclic Graph) cho Apache Airflow. Nó đóng vai trò là "nhạc trưởng", điều phối khi nào thì code Python lấy dữ liệu được chạy.

## Thành phần
- **`pipeline.py`**: Định nghĩa luồng chạy tự động `weather_meteo_elt_pipeline`.
  - **Schedule:** `@hourly` (Chạy mỗi giờ 1 lần).
  - **Retry:** Tự động retry 2 lần nếu gọi API thất bại.
  - **Task chính:** Nhúng logic Python (`src.main`) thông qua `PythonOperator`.

## Lưu ý khi triển khai
- Thư mục này được mount trực tiếp vào container Airflow thông qua Docker Volume (`./airflow/dags:/opt/airflow/dags`). Mọi thay đổi trong code ở đây sẽ được Airflow nhận diện ngay lập tức mà không cần restart container.
- Đảm bảo biến môi trường `PYTHONPATH` của Airflow đã có thư mục gốc của project để DAG có thể import được các module từ `src/`.
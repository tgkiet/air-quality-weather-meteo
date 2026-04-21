# ⏰ Airflow DAGs (Orchestration Layer)

Thư mục này chứa các file định nghĩa **DAG (Directed Acyclic Graph)** để cung cấp cho Apache Airflow.

Airflow đóng vai trò như một người **nhạc trưởng (Orchestrator)**. Thay vì bạn phải mở Terminal lên gõ `python src/main.py` mỗi ngày, Airflow sẽ tự động thực hiện việc đó theo một lịch trình bạn định sẵn (Ví dụ: 1 tiếng chạy 1 lần).

## 📄 Các file DAG
- `pipeline.py`: Chứa DAG `weather_meteo_elt_pipeline`. 
  - Nó import trực tiếp code từ thư mục `src/` (`OpenMeteoExtractor`, `PostgresLoader`).
  - Định nghĩa một `PythonOperator` để chạy luồng quy trình: Kéo API -> Cắm vào Database.
  - Được cấu hình chạy định kỳ (schedule_interval).

## 💡 Cách hoạt động
Airflow sẽ dò tìm trong thư mục này. Bất cứ file Python nào định nghĩa một Object `DAG` thì sẽ hiển thị lên giao diện Web UI của Airflow. Từ Web UI, bạn có thể theo dõi lịch sử chạy, số lần lỗi (retry), và chi tiết từng task trong chu trình.
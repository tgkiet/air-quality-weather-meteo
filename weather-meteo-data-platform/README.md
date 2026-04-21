# Data Platform Core

Thư mục này là **trái tim** của toàn bộ hệ thống xử lý dữ liệu. Nó bao gồm toàn bộ code và config để vận hành quy trình ELT (Extract, Load, Transform).

## Công Nghệ Sử Dụng
- **Ngôn ngữ:** Python 3.x
- **Database (Data Warehouse):** PostgreSQL
- **Orchestration:** Apache Airflow
- **Data Transformation:** dbt (Data Build Tool)
- **Containerization:** Docker & Docker Compose

## Cấu Trúc Thành Phần
1. [`src/`](./src/README.md): Chứa code Python thực hiện khâu **Extract** (Gọi Open-Meteo API) và **Load** (Nhét raw JSON vào PostgreSQL).
2. [`dags/`](./dags/README.md): Chứa các kịch bản lập lịch (DAGs) để Airflow tự động hoá việc chạy code ở thư mục `src`.
3. [`dbt-transform/`](./dbt-transform/): Chứa dự án `dbt`. Làm nhiệm vụ **Transform**: query vào bảng raw JSON trong PostgreSQL để bóc tách, làm sạch (Staging) và thiết kế bảng phân tích (Marts).

## Hướng Dẫn Chạy (Quick Start)
### Chạy test ở Local (Chỉ Extract và Load)
Nếu chỉ muốn thử nghiệm việc lấy dữ liệu và lưu vào DB mà không cần bật Airflow:
```bash
# 1. Khởi tạo Database (đảm bảo bảng api_openmeteo_raw_data đã được tạo)
# 2. Chạy pipeline:
python src/main.py
```
*(đã cấu hình các biến môi trường trong file `.env`) chưa???*
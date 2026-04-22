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

### 1. Chuẩn bị biến môi trường
Tạo file `.env` tại thư mục này với nội dung sau:
```env
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=air_quality_db
POSTGRES_HOST=localhost
POSTGRES_PORT=5434

AIRFLOW_USER=airflow_user
AIRFLOW_PASSWORD=airflow_password
AIRFLOW_DB=airflow_db
```

### 2. Khởi chạy toàn bộ hệ thống bằng Docker
Bạn chỉ cần gõ lệnh sau để dựng PostgreSQL và Airflow lên:
```bash
docker compose up -d
```
*Lưu ý: Trong lần khởi chạy đầu tiên, Postgres sẽ tự động tạo ra cả `air_quality_db` và `airflow_db` dựa trên file script `src/scripts/init_dbs.sh`.*

### 3. Đăng nhập Airflow
- Truy cập trình duyệt: `http://localhost:8080`
- **Username:** `admin`
- **Password:** Lấy từ log của Airflow container (`docker logs airflow_container | grep Password`) hoặc file `standalone_admin_password.txt`.

### 4. Chạy Test Local (Không dùng Airflow)
Nếu bạn muốn debug trực tiếp quá trình Extract và Load:
```bash
python src/main.py
```
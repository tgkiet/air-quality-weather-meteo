# Data Platform Core

Thư mục này là **trái tim** của toàn bộ hệ thống xử lý dữ liệu. Nó bao gồm toàn bộ code và config để vận hành quy trình ELT (Extract, Load, Transform).

## Công Nghệ Sử Dụng
- **Ngôn ngữ:** Python 3.x
- **Database (Data Warehouse):** PostgreSQL
- **Orchestration:** Apache Airflow
- **Data Transformation:** dbt (Data Build Tool)
- **Containerization:** Docker & Docker Compose

## Cấu Trúc Thành Phần
1. [`src/`](./src/README.md): Chứa code Python thực hiện khâu **Extract** (Gọi Open-Meteo API) và **Load** (Nhét raw JSON vào PostgreSQL). Có tích hợp hệ thống Logging (`logs/pipeline.log`) và bảo mật chống SQL Injection.
2. [`airflow/`](./airflow/): Thư mục cấu hình cho Airflow, chứa `dags/` (kịch bản lập lịch tự động) và `logs/`.
3. [`dbt-transform/`](./dbt-transform/): Chứa dự án `dbt`. Làm nhiệm vụ **Transform**: query vào bảng raw JSON trong PostgreSQL để bóc tách, làm sạch (Staging - Silver) và thiết kế bảng phân tích (Marts - Gold).
4. `Dockerfile` & `requirements_airflow.txt`: File thiết lập Custom Image cho Airflow, giúp cài đặt sẵn các thư viện cần thiết (Production Best Practice), tách biệt với môi trường Local.

> **💡 Lưu ý về Dữ liệu Raw (Bronze Layer):** 
> Dữ liệu ở tầng Raw sử dụng cơ chế **Append-only** (Chỉ thêm mới). Do đó, việc bạn thấy nhiều record trùng lặp khi chạy pipeline nhiều lần là **BÌNH THƯỜNG**. Việc làm sạch và xóa trùng lặp (Deduplication) sẽ được `dbt` xử lý ở tầng Silver.

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
Vì hệ thống sử dụng Custom Image cho Airflow, bạn luôn phải thêm cờ `--build` để cài đặt các thư viện từ `requirements_airflow.txt`:
```bash
docker compose down
docker compose up -d --build
```
*(Cảnh báo: Không thêm cờ `-v` vào lệnh `down` trừ khi bạn muốn xóa vĩnh viễn toàn bộ dữ liệu Database).*

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

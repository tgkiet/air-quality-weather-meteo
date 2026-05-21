# loaders/ — Lớp Tải Dữ Liệu (Load Layer)

## Nhiệm vụ
Chịu trách nhiệm **duy nhất**: tiếp nhận dữ liệu (từ API raw JSON hoặc từ file CSV lịch sử) và ghi an toàn vào PostgreSQL (Bronze Layer). Lớp này hoàn toàn tuân thủ lập trình hướng đối tượng (OOP) và cơ chế DRY (Don't Repeat Yourself).

---

## File: `base_loader.py` — `class BasePostgresLoader` (Lớp Cơ Sở)

Lớp cha cung cấp các hành vi dùng chung cho tất cả các Loader kết nối với PostgreSQL database.

### Khởi tạo (`__init__()`)
* **Fail-fast Validation:** Đọc và kiểm tra lập tức 5 biến môi trường cơ sở dữ liệu (`POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`). Nếu thiếu bất kỳ biến nào, quăng lỗi `EnvironmentError` ngay từ lúc khởi tạo đối tượng.

### Hàm `connect()`
* **Idempotent Connect:** Tránh tình trạng mở nhiều connection dư thừa bằng cách kiểm tra trạng thái đóng/mở của connection hiện tại trước khi khởi tạo mới.
* **Retry với Exponential Backoff:** Đọc số lần thử và thời gian giãn cách từ cấu hình của hệ thống (`config_manager.database_config`), tự động phục hồi khi gặp sự cố ngắt kết nối mạng tạm thời.

### Hàm `close()`
* Đóng kết nối an toàn sau khi kết thúc tác vụ, giải phóng connection pool của PostgreSQL.

---

## File: `postgres_loader.py` — `class PostgresLoader` (Kế thừa `BasePostgresLoader`)

Chuyên trách việc tải dữ liệu JSON thô nhận từ các API Extractor vào bảng raw `api_openmeteo_raw_data`.

### Hàm `insert_data(table_name, source_type, execution_date, raw_json)`
* **Idempotency (Tính lũy đẳng):** Sử dụng mệnh đề `ON CONFLICT (source_type, execution_date) DO UPDATE SET raw_json = EXCLUDED.raw_json` để đảm bảo khi chạy lại một mẻ dữ liệu của Airflow (cùng logical execution date), hệ thống sẽ cập nhật đè lên bản ghi cũ chứ không tạo bản ghi trùng lặp.
* **Chống SQL Injection:** Sử dụng `psycopg2.sql.Identifier` để bảo vệ tên bảng khỏi các lỗ hổng chèn ép SQL.
* **Hiệu năng:** Dùng `psycopg2.extras.Json` để tối ưu hóa việc serialize dữ liệu JSONB của Postgres.

---

## File: `csv_loader.py` — `class CSVLoader` (Kế thừa `BasePostgresLoader`)

Chuyên trách việc nạp các tệp dữ liệu CSV lịch sử lớn (>800k dòng) vào bảng `bronze_historical_weather`.

### Hàm `create_table_if_not_exists()`
* Tạo bảng `bronze_historical_weather` với cấu trúc đầy đủ các trường thời tiết và chất lượng không khí thô.
* Thêm ràng buộc `UNIQUE (datetime, lat, lon)` để làm mỏ neo cho cơ chế kiểm soát trùng lặp dữ liệu (Idempotency) khi nạp lại.

### Hàm `load_csv(csv_file_path)`
Nạp dữ liệu hiệu năng cao tích hợp 3 kỹ thuật nâng cao:
1. **Dynamic Header Mapping:** Đọc dòng header đầu tiên của tệp CSV để sinh câu lệnh `COPY` khớp chính xác thứ tự các cột trong CSV với cột database tương ứng. Giải quyết triệt để hạn chế của Postgres COPY (chỉ ánh xạ theo thứ tự vị trí cột chứ không ánh xạ theo tên tiêu đề).
2. **PostgreSQL COPY EXPERT:** Stream trực tiếp dữ liệu từ file CSV của client lên server thông qua `STDIN`. Giúp tốc độ nạp nhanh hơn gấp 10-20 lần so với câu lệnh `INSERT` thông thường và sử dụng lượng RAM cực kỳ nhỏ (O(1) memory).
3. **Temp Table + Set-based UPSERT:**
   * Nạp nhanh toàn bộ CSV vào một bảng tạm thời (`temp_historical_weather`) có cấu trúc giống hệt bảng chính.
   * Chuyển dữ liệu từ bảng tạm sang bảng chính bằng câu lệnh `INSERT ... SELECT ... ON CONFLICT (datetime, lat, lon) DO UPDATE SET ...`
   * Bằng cách này, toàn bộ quá trình so khớp và ghi đè trùng lặp được xử lý trực tiếp ở tầng Database ở cấp độ set-based, loại bỏ các vòng lặp Python tốn thời gian.

---

## Sơ đồ lớp (Class Diagram)

```mermaid
classDiagram
    class BasePostgresLoader {
        +connection
        +db_name
        +db_user
        +connect()
        +close()
    }
    class PostgresLoader {
        +insert_data()
    }
    class CSVLoader {
        +create_table_if_not_exists()
        +load_csv()
    }
    BasePostgresLoader <|-- PostgresLoader : Kế thừa
    BasePostgresLoader <|-- CSVLoader : Kế thừa
```

# scripts/ — Khởi Tạo Hệ Thống

## Nhiệm vụ
Chứa các script hệ thống chạy một lần khi khởi tạo infrastructure. Không phải code Python pipeline.

---

## `init_dbs.sh` — Khởi Tạo Database

**Cơ chế kích hoạt:** PostgreSQL tự động chạy mọi file trong `/docker-entrypoint-initdb.d/` **đúng một lần duy nhất** khi volume data chưa tồn tại (lần đầu `docker compose up` hoặc sau `docker compose down -v`).

### Phần 1: Airflow Database
```sql
CREATE USER $AIRFLOW_DB_USER WITH PASSWORD '...';
CREATE DATABASE $AIRFLOW_DB_NAME;
GRANT ALL PRIVILEGES ON DATABASE $AIRFLOW_DB_NAME TO $AIRFLOW_DB_USER;
GRANT ALL ON SCHEMA public TO $AIRFLOW_DB_USER;  -- Postgres 15+ yêu cầu
```

**Tại sao tách DB riêng cho Airflow?**
- `air_quality_db` = Data Warehouse thật (backup định kỳ)
- `airflow_db` = Metadata DAG/Task của Airflow (có thể tái tạo)
- Tách riêng: bảo mật độc lập, backup riêng biệt, restore không ảnh hưởng lẫn nhau

### Phần 2: Data Warehouse Schema (Bronze Layer)
```sql
CREATE TABLE IF NOT EXISTS api_openmeteo_raw_data (
    id              SERIAL PRIMARY KEY,
    source_type     VARCHAR(100) NOT NULL,   -- 'weather_forecast_hourly' | 'air_quality_hourly'
    execution_date  TIMESTAMPTZ  NOT NULL,   -- Logical Date của Airflow, KHÔNG phải datetime.now()
    raw_json        JSONB        NOT NULL,   -- JSON nguyên vẹn từ API
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()  -- Timestamp ghi thực tế
);

-- Mỏ neo Idempotency — BẮT BUỘC để UPSERT hoạt động
ALTER TABLE api_openmeteo_raw_data
ADD CONSTRAINT unique_source_execution_date
UNIQUE (source_type, execution_date);
```

**Tại sao cần `UNIQUE CONSTRAINT`?**
`PostgresLoader` dùng câu lệnh `INSERT ... ON CONFLICT (source_type, execution_date) DO UPDATE`. Nếu không có Constraint này, PostgreSQL sẽ throw lỗi: `"There is no unique constraint matching the ON CONFLICT specification"`.

**Ý nghĩa của từng cột:**
| Cột | Kiểu | Mục đích |
|---|---|---|
| `source_type` | `VARCHAR` | Phân biệt nguồn dữ liệu |
| `execution_date` | `TIMESTAMPTZ` | Khóa tự nhiên cho Idempotency |
| `raw_json` | `JSONB` | Lưu nguyên vẹn, biến đổi sau ở Silver layer |
| `ingested_at` | `TIMESTAMPTZ` | Audit trail — khi nào data thực sự được ghi |

---

## Các Python Scripts nạp dữ liệu lịch sử (Historical Loader & Backfiller)

Ngoài việc cào dữ liệu thời gian thực hàng giờ, hệ thống hỗ trợ 2 cơ chế nạp bù dữ liệu quá khứ (Backfill) từ 02/08/2022 đến 29/11/2025:

### 1. `load_historical_csvs.py` (Hà Nội)
* **Mục đích:** Nạp dữ liệu lịch sử của 31 trạm quan trắc Hà Nội từ file CSV có sẵn.
* **Cơ chế:** Sử dụng class `CSVLoader` kế thừa từ `BasePostgresLoader`.
* **Kỹ thuật tối ưu:** Sử dụng Postgres `COPY EXPERT` thông qua Temporary Table, thực hiện UPSERT theo lô (batch) từ Temp Table vào bảng chính. Đạt tốc độ nạp nhanh gấp 10-20 lần so với INSERT thông thường, giải quyết 900.000 dòng dữ liệu trong 3 giây.
* **Cách chạy:**
  ```bash
  docker exec -i airflow_container python3 /opt/airflow/src/scripts/load_historical_csvs.py
  ```

### 2. `backfill_hcm_history.py` (TP.HCM)
* **Mục đích:** Nạp dữ liệu lịch sử của 22 quận/huyện TP.HCM từ API Lịch sử của Open-Meteo.
* **Cơ chế:** Kết nối tới Open-Meteo Archive API và Air Quality API để lấy toàn bộ dữ liệu lịch sử từ 02/08/2022 đến 29/11/2025.
* **Kỹ thuật tối ưu:**
  * Đồng bộ hóa (Join) dữ liệu thời tiết và chất lượng không khí theo mốc thời gian bằng Python trước khi nạp.
  * Tự động gán ID địa điểm mới bắt đầu từ `3000000+` để tránh xung đột với ID trạm Hà Nội.
  * Sử dụng `execute_values` (UPSERT) kết hợp với `AT TIME ZONE 'Asia/Bangkok'` để chuẩn hóa múi giờ gốc của dữ liệu trước khi đẩy vào Postgres `TIMESTAMPTZ`.
* **Cách chạy:**
  ```bash
  docker exec -i airflow_container python3 /opt/airflow/src/scripts/backfill_hcm_history.py
  ```

---

> **Lưu ý chung:** Dùng file `.sh` thay vì `.sql` vì `.sh` có thể đọc biến môi trường `$AIRFLOW_DB_USER` từ `.env`. File `.sql` thuần không làm được điều này.

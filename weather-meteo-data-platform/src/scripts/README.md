# scripts/ — System Scripts

## Nhiệm vụ
Chứa các script khởi tạo infrastructure, nạp dữ liệu lịch sử (backfill) và toàn bộ các **Consumption Apps** (Interactive Bot & Push Alerts) để phân phối dữ liệu tới người dùng cuối.

---

## `init_dbs.sh` — Khởi Tạo Database

**Cơ chế kích hoạt:** PostgreSQL tự động chạy mọi file trong `/docker-entrypoint-initdb.d/` **đúng một lần duy nhất** khi volume data chưa tồn tại (lần đầu `docker compose up` hoặc sau `docker compose down -v`).

> Dùng file `.sh` thay vì `.sql` vì `.sh` đọc được biến môi trường `$AIRFLOW_DB_USER` từ `.env`. File `.sql` thuần không làm được điều này.

### Phần 1: Tạo Database Airflow riêng

```sql
CREATE USER $AIRFLOW_DB_USER WITH PASSWORD '...';
CREATE DATABASE $AIRFLOW_DB_NAME;
GRANT ALL PRIVILEGES ON DATABASE $AIRFLOW_DB_NAME TO $AIRFLOW_DB_USER;
GRANT ALL ON SCHEMA public TO $AIRFLOW_DB_USER;  -- Postgres 15+ yêu cầu
```

**Tại sao tách DB riêng cho Airflow?**
- `air_quality_db` = Data Warehouse thật → backup định kỳ, giữ lịch sử lâu dài
- `airflow_db` = Metadata DAG/Task → có thể tái tạo bất kỳ lúc nào
- Tách riêng: bảo mật độc lập, backup riêng biệt, restore không ảnh hưởng lẫn nhau

### Phần 2: Bronze Layer Schema + UNIQUE Constraints

```sql
-- Bảng realtime (Open-Meteo Forecast API, cập nhật mỗi giờ)
CREATE TABLE IF NOT EXISTS api_openmeteo_raw_data (
    id              SERIAL PRIMARY KEY,
    source_type     VARCHAR(100) NOT NULL,  -- 'weather_forecast_hourly' | 'air_quality_hourly'
    execution_date  TIMESTAMPTZ  NOT NULL,  -- Logical Date Airflow — KHÔNG phải NOW()
    raw_json        JSONB        NOT NULL,  -- JSON nguyên vẹn từ API
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
-- Mỏ neo Idempotency: PostgresLoader dùng ON CONFLICT (source_type, execution_date)
ALTER TABLE api_openmeteo_raw_data
ADD CONSTRAINT unique_source_execution_date UNIQUE (source_type, execution_date);

-- Bảng historical (Archive API backfill cho toàn bộ 52 Quận/Huyện)
CREATE TABLE IF NOT EXISTS bronze_historical_weather (
    id                    SERIAL PRIMARY KEY,
    datetime              TIMESTAMPTZ NOT NULL,
    temperature_2m        NUMERIC,
    relative_humidity_2m  NUMERIC,
    precipitation         NUMERIC,
    rain                  NUMERIC,
    wind_speed_10m        NUMERIC,
    wind_direction_10m    NUMERIC,
    pressure_msl          NUMERIC,
    boundary_layer_height NUMERIC,
    pm10_cams             NUMERIC,
    pm2_5_cams            NUMERIC,
    carbon_monoxide_cams  NUMERIC,
    nitrogen_dioxide_cams NUMERIC,
    sulphur_dioxide_cams  NUMERIC,
    ozone_cams            NUMERIC,
    location_id           NUMERIC,
    lat                   NUMERIC,
    lon                   NUMERIC,
    location_name         VARCHAR(255),  -- "HN Đống Đa" / "HCM Quận 1" — luôn có giá trị với dữ liệu API
    ingested_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Mỏ neo Idempotency: backfill_history.py dùng ON CONFLICT
ALTER TABLE bronze_historical_weather
ADD CONSTRAINT unique_historical_datetime_lat_lon UNIQUE (datetime, lat, lon);
```

---
---

## `backfill_history.py` — Backfill từ Archive API

**Mục đích:** Nạp dữ liệu lịch sử Weather + Air Quality từ Open-Meteo Archive API. Hỗ trợ nạp toàn bộ 52 Quận/Huyện (30 HN + 22 HCM).

### Tham Số

```bash
python3 backfill_history.py \
    --location-prefix {HCM|HN} \   # Bắt buộc
    --start-date YYYY-MM-DD \       # Bắt buộc
    --end-date   YYYY-MM-DD         # Bắt buộc
```

### Cách Dùng — 2 Trường Hợp

**Trường hợp 1: Backfill TP.HCM toàn bộ (22 Quận/Huyện)**
```bash
docker exec airflow_container \
    python3 /opt/airflow/src/scripts/backfill_history.py \
    --location-prefix HCM \
    --start-date 2022-08-02 --end-date 2026-05-27
```
~22 locations × ~3.8 năm → **~5-10 phút**

**Trường hợp 2: Backfill Hà Nội toàn bộ (30 Quận/Huyện/Thị xã)**
```bash
docker exec airflow_container \
    python3 /opt/airflow/src/scripts/backfill_history.py \
    --location-prefix HN \
    --start-date 2022-08-02 --end-date 2026-05-27
```
~30 locations × ~3.8 năm → **~10-15 phút**

### Kỹ Thuật Quan Trọng

| Fix | Mô Tả |
|---|---|
| Feature | Align AQ/Weather theo `time_str` dict key, không phải positional index → tránh gán PM2.5 sai giờ |
| Feature | `safe_get()` guard IndexError khi API trả về array ngắn hơn time array |
| Feature | `%s::TIMESTAMP AT TIME ZONE 'Asia/Bangkok'` → timezone đúng khi insert vào TIMESTAMPTZ |
| **Retry** | Exponential backoff: 4xx không retry, 429/5xx retry tối đa 3 lần |
| **Idempotent** | `ON CONFLICT (datetime, lat, lon) DO UPDATE` → chạy lại không duplicate |

### Location ID Offset

| Prefix | ID Offset | Lý Do |
|---|---|---|
| HCM backfill | 3000000 + idx | Tránh conflict với HN backfill |
| HN backfill | 4000000 + idx | Tránh conflict với HCM |

---

## `telegram_bot.py` & `bot_services.py` — Lõi Interactive Pull Bot

**Mục đích:** Cung cấp giao diện tra cứu Thời tiết & AQI chủ động qua Telegram.

- **`telegram_bot.py` (Controller):** Tuân thủ tuyệt đối OOP. Quản lý State (ngôn ngữ EN/VI) và Inline Keyboard Menu. Phân luồng Request người dùng nhưng **không** xử lý DB hay Format text.
- **`bot_services.py` (Service Layer):**
    - `BotDatabaseManager`: Khởi tạo và truy xuất bảng `bot_user_preferences`, query dữ liệu từ tầng Gold (Data Mart).
    - `BotFormatter`: Xử lý triệt để các rào cản UX (VD: "0.0mm Paradox" - ẩn tỉ lệ mưa nếu lượng mưa = 0). Định dạng tin nhắn song ngữ chuẩn Micro-Dashboard phẳng (Không dùng icon emoji rườm rà).

## `alert_job.py` — Lõi Broadcast Push Alert

**Mục đích:** Cronjob định kỳ chạy sau `dbt test` để quét Data Mart và phát thanh tin nhắn khẩn.
- Được thiết kế với kiến trúc **Dual-Core Push**:
    1. **Bản tin Tối (20:00)**: Tổng hợp rủi ro mưa lớn Ngày Mai.
    2. **Bản tin Sáng (06:00)**: Khuyến cáo AQI trong 24h tới.
    3. **Cảnh báo Đột xuất (Khung giờ còn lại)**: Chống Spam bằng **Stateful Deduplication** (lưu trạng thái vào `silver_layer.alert_history`), đảm bảo một sự kiện mưa lớn trong 6H tới chỉ "réo" người dùng 1 lần duy nhất.
- Đã được refactor triệt để **Zero Hardcode**: Toàn bộ điều kiện query lượng mưa và AQI được lấy động từ `config_manager`, độc lập hoàn toàn với cờ tĩnh của dbt.

---

## Thứ Tự Chạy Khi Setup Lần Đầu

```bash
# 1. Khởi động hệ thống (init_dbs.sh chạy tự động)
docker compose up -d --build

# 2. Backfill dữ liệu lịch sử cho toàn bộ Hà Nội (30 Quận/Huyện)
docker exec airflow_container \
    python3 /opt/airflow/src/scripts/backfill_history.py \
    --location-prefix HN \
    --start-date 2022-08-02 --end-date 2026-05-27

# 3. Backfill dữ liệu lịch sử cho toàn bộ TP.HCM (22 Quận/Huyện)
docker exec airflow_container \
    python3 /opt/airflow/src/scripts/backfill_history.py \
    --location-prefix HCM \
    --start-date 2022-08-02 --end-date 2026-05-27

# 4. Rebuild Silver + Gold với toàn bộ historical data
docker exec airflow_container bash -c \
    "dbt run --full-refresh \
     --project-dir /opt/airflow/dbt-transform \
     --profiles-dir /home/airflow/.dbt"
```

> **Idempotent:** Tất cả các bước đều có thể chạy lại mà không tạo duplicate.

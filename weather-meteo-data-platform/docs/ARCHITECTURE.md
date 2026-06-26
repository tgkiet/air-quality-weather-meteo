# Kiến Trúc Hệ Thống

Tài liệu này mô tả chi tiết kiến trúc dữ liệu, luồng xử lý và các quyết định kỹ thuật của hệ thống.

---

## Tổng Quan ELT Pipeline

Hệ thống áp dụng mô hình **ELT** (Extract → Load → Transform), tách biệt hoàn toàn 3 giai đoạn:

```
[Open-Meteo API]
      │ HTTP GET (mỗi giờ)
      ▼
┌─────────────────────────────────────┐
│  EXTRACT LAYER  (src/extractors/)   │
│  OpenMeteoExtractor                 │
│  - Weather API: 14 biến thời tiết   │
│  - Air Quality API: 9 biến không khí│
│  - Retry tối đa 3 lần nếu lỗi mạng  │
└──────────────┬──────────────────────┘
               │ raw JSON dict
               ▼
┌─────────────────────────────────────┐
│  LOAD LAYER     (src/loaders/)      │
│  PostgresLoader                     │
│  - INSERT INTO api_openmeteo_raw_data│
│  - UPSERT (ON CONFLICT DO UPDATE)   │
│  - Idempotent: chạy lại không dup  │
│  - Bảo vệ SQL Injection (Identifier)│
└──────────────┬──────────────────────┘
               │ raw JSONB trong Postgres
               ▼
┌─────────────────────────────────────┐
│  TRANSFORM LAYER  (dbt-transform/)  │
│  dbt (Data Build Tool)              │
│  - Dedup bằng ROW_NUMBER()          │
│  - Parse JSON → tabular columns     │
│  - Cast kiểu dữ liệu, xử lý NULL   │
│  - Join weather + air quality       │
└─────────────────────────────────────┘
```

---

## Medallion Architecture (Bronze → Silver → Gold)

### Bronze Layer — Raw Data

| Thuộc Tính | Giá Trị |
|---|---|
| **Bảng** | `api_openmeteo_raw_data` |
| **Database** | `air_quality_db` |
| **Kiểu lưu trữ** | JSONB (Binary JSON) |
| **Cơ chế ghi** | Idempotent UPSERT (ON CONFLICT DO UPDATE) |
| **Retention** | Vĩnh viễn |

```sql
CREATE TABLE api_openmeteo_raw_data (
    id          SERIAL PRIMARY KEY,
    source_type VARCHAR(50) NOT NULL,   -- 'weather_forecast_hourly' | 'air_quality_hourly'
    raw_json    JSONB NOT NULL,         -- Toàn bộ response JSON từ API
    ingested_at TIMESTAMP DEFAULT NOW() -- Thời điểm dữ liệu được nạp vào
);
```

> **Tại sao UPSERT?** Để đảm bảo tính Idempotency (Lũy đẳng). Khi Airflow chạy lại (retry) một khoảng thời gian, hệ thống sẽ ghi đè record cũ thay vì tạo ra dữ liệu trùng lặp, giữ cho Bronze Layer luôn sạch và ổn định.

> **Tại sao JSONB?** PostgreSQL JSONB cho phép lưu response API mà không cần schema cứng, đồng thời hỗ trợ index và query trực tiếp vào các field JSON với toán tử `->` và `->>`.

### Silver Layer — Staging

Models dbt trong `models/staging/`:

- **`stg_weather_hourly`**: Bóc tách JSON thời tiết, 1 row = 1 giờ đo
- **`stg_air_quality_hourly`**: Bóc tách JSON chất lượng không khí, 1 row = 1 giờ đo

Các phép biến đổi áp dụng:
1. **Deduplication** bằng `DISTINCT ON (forecast_time, lat, lon) ... ORDER BY execution_date DESC` — lấy bản ghi mới nhất khi có trùng lặp khóa chính
2. **JSON Parsing** — trải JSONB ra thành cột độc lập bằng toán tử `->>`
3. **Type Casting** — `TEXT` → `FLOAT`, `TEXT` → `TIMESTAMPTZ`
4. **Renaming** — đổi tên cột theo convention `snake_case`

### Gold Layer — Marts

Models dbt trong `models/marts/`:

- **`mart_hourly_conditions`**: JOIN bảng thời tiết + chất lượng không khí theo `forecast_time` và `(latitude, longitude)`, sẵn sàng cho BI Dashboard.

---

## Kiến Trúc Docker

```
┌─────────────────────────────── meteo_network ───────────────────────┐
│                                                                      │
│  ┌─────────────────────────┐       ┌──────────────────────────────┐ │
│  │    postgres_container   │       │      airflow_container       │ │
│  │    (postgres:16-alpine) │◄──────│  (Custom: airflow:3.2.0 +   │ │
│  │                         │       │   psycopg2 + requests + ...) │ │
│  │  Port: 5432 (internal)  │       │                              │ │
│  │  Port: 5434 (host)      │       │  Port: 8080 (host & cont.)  │ │
│  │                         │       │                              │ │
│  │  Volumes:               │       │  Processes:                  │ │
│  │  - postgres_data (named)│       │  - airflow scheduler         │ │
│  │  - init_dbs.sh (bind)   │       │  - airflow triggerer         │ │
│  │                         │       │  - airflow dag-processor     │ │
│  │  Databases:             │       │  - airflow api-server        │ │
│  │  - air_quality_db       │       │                              │ │
│  │  - airflow_db           │       │  Volumes (bind):             │ │
│  └─────────────────────────┘       │  - ./airflow/dags → /dags   │ │
│                                    │  - ./airflow/logs → /logs   │ │
│                                    │  - ./src → /opt/airflow/src │ │
│                                    │  - ./dbt-transform (dbt)    │ │
│                                    │  - ./.dbt (profiles.yml)    │ │
│                                    └──────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

**Tách biệt 2 Database (Security Best Practice):**
- `air_quality_db` — Data Warehouse, chỉ user data pipeline truy cập
- `airflow_db` — Airflow metadata, chỉ user Airflow truy cập
- Hai user khác nhau, hai password khác nhau → breach một DB không ảnh hưởng DB còn lại

---

## Luồng Dữ Liệu Chi Tiết

```
[Airflow Scheduler]
        │ trigger @hourly (0 * * * *)
        ▼
[BashOperator: fetch_data]
        │ bash_command: "python3 /opt/airflow/src/main.py"
        ▼
[src/main.py]
        │
        ├── ConfigManager().get_config()
        │       └── đọc src/config/config.json (Singleton, chỉ đọc 1 lần)
        │
        ├── [1. EXTRACT Weather]
        │       OpenMeteoExtractor(weather_url)
        │       .get_open_meteo_data(weather_params)
        │       └── HTTP GET → api.open-meteo.com
        │           retry 3 lần × 5s delay nếu lỗi
        │           └── raise Exception nếu vẫn thất bại
        │
        ├── [2. EXTRACT Air Quality]
        │       OpenMeteoExtractor(aq_url)
        │       .get_open_meteo_data(aq_params)
        │       └── HTTP GET → air-quality-api.open-meteo.com
        │
        └── [3. LOAD vào PostgreSQL]
                PostgresLoader()
                .connect()
                .insert_data("api_openmeteo_raw_data", ...)
                .close()
                └── Kết quả: +2 rows trong Bronze table
        
        ▼
[BashOperator: dbt_run]
        │ bash_command: "dbt run --project-dir ... --profiles-dir ..."
        ├── 1. Staging (Tạo View, Flatten JSON)
        ├── 2. Silver (Incremental Merge, Dedup dữ liệu)
        └── 3. Gold (Denormalize, LEFT JOIN tạo Data Marts)
        
        ▼
[BashOperator: dbt_test]
        │ bash_command: "dbt test --project-dir ... --profiles-dir ..."
        └── Chạy 32 bài Data Quality Tests (NotNull, Unique, AcceptedValues).
            Nếu Pass → Pipeline SUCCESS. Nếu Fail → Pipeline FAILED.
            
        ▼ (Nếu dbt_test PASS)
[BashOperator: alert_job]
        │ bash_command: "python3 /opt/airflow/src/scripts/alert_job.py --execution_date {{ logical_date }}"
        └── Lõi Push Bot (Dual-Core):
            ├── 06:00: Phát thanh "Bản tin Sáng" (Toàn bộ Rủi ro Hôm nay)
            ├── 20:00: Phát thanh "Bản tin Tối" (Toàn bộ Rủi ro Ngày mai)
            ├── Giờ khác: Quét đột xuất 6H tới (Stateful Deduplication chống Spam)
            └── Phân lớp lượng mưa tự động (Mưa lớn >=5mm, Mưa vừa >=3mm)

---
[Độc lập bên ngoài Airflow]
[Telegram Polling Bot Container]
        │ Chạy ngầm liên tục (`infinity_polling`)
        └── Lõi Pull Bot (Interactive):
            ├── Trả lời Query của người dùng theo thời gian thực
            ├── Áp dụng Phân trang (Pagination) in-memory an toàn
            ├── Xử lý Concurrent Requests với Stateless Direct Database Connection giúp triệt tiêu TCP Idle Timeout (kết hợp 4 bộ dbt Native Indexes cho tốc độ <1ms)
            └── Clean UI qua `edit_message_text`
```

**Nếu bất kỳ bước nào raise Exception:**
→ Python exit code ≠ 0  
→ Airflow đánh dấu task **FAILED**  
→ Airflow tự **retry** tùy chỉnh (vd: 2 lần)
→ Nếu vẫn FAILED → gửi alert (nếu cấu hình)

---

## Dữ Liệu Thu Thập

### Thời Tiết — `source_type: weather_forecast_hourly`

| Biến | Đơn Vị | Mô Tả |
|---|---|---|
| `temperature_2m` | °C | Nhiệt độ tại độ cao 2m |
| `relative_humidity_2m` | % | Độ ẩm tương đối |
| `dew_point_2m` | °C | Điểm sương |
| `apparent_temperature` | °C | Nhiệt độ cảm nhận |
| `precipitation_probability` | % | Xác suất có mưa |
| `precipitation` | mm | Lượng mưa |
| `pressure_msl` | hPa | Áp suất khí quyển |
| `surface_pressure` | hPa | Áp suất bề mặt |
| `cloud_cover` | % | Độ phủ mây |
| `visibility` | m | Tầm nhìn (Chỉ có ở Realtime Forecast, NULL ở Historical) |
| `wind_speed_10m` | km/h | Tốc độ gió |
| `wind_direction_10m` | ° | Hướng gió |
| `wind_gusts_10m` | km/h | Gió giật |
| `uv_index` | — | Chỉ số tia UV |

### Chất Lượng Không Khí — `source_type: air_quality_hourly`

| Biến | Đơn Vị | Mô Tả |
|---|---|---|
| `pm10` | μg/m³ | Bụi hạt mịn PM10 |
| `pm2_5` | μg/m³ | Bụi hạt siêu mịn PM2.5 |
| `carbon_monoxide` | μg/m³ | Khí CO |
| `nitrogen_dioxide` | μg/m³ | Khí NO₂ |
| `sulphur_dioxide` | μg/m³ | Khí SO₂ |
| `ozone` | μg/m³ | Ozone tầng mặt đất |
| `aerosol_optical_depth` | — | Độ đục quang học không khí |
| `dust` | μg/m³ | Bụi thô |
| `uv_index` | — | Chỉ số tia UV |

---

## Quyết Định Kỹ Thuật Quan Trọng

### Tại Sao `@hourly` Thay Vì `timedelta(hours=1)`?
- `timedelta(hours=1)`: Đếm 1 giờ từ lúc bật → chạy vào phút lẻ (10:13, 11:13...)
- `@hourly`: Chạy đúng phút 0 mỗi giờ → dữ liệu chuẩn hóa theo time series

### Tại Sao FabAuthManager Thay Vì SimpleAuthManager (Airflow 3 Default)?
- `SimpleAuthManager` mặc định của Airflow 3 chỉ cho 1 admin với password random
- `FabAuthManager` hỗ trợ RBAC đầy đủ, tạo user bằng CLI, tích hợp OAuth

### Tại Sao `raise` Thay Vì `return` Trong Error Handler?
- `return` → Python exit code = 0 → Airflow đánh dấu SUCCESS giả
- `raise` → Python exit code ≠ 0 → Airflow biết FAILED → kích hoạt retry

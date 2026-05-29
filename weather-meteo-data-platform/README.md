# 🌤️ Vietnam Weather & Air Quality Data Platform 🍃

> **Enterprise-grade Data Engineering pipeline** tự động thu thập, chuẩn hóa và phân tích dữ liệu **thời tiết & chất lượng không khí** cho **52 khu vực quan trắc** trên 2 thành phố — **30 Quận/Huyện Hà Nội** và **22 Quận/Huyện TP.HCM** — cập nhật mỗi giờ và lưu trữ lịch sử từ năm 2022.
>

---

## Kiến Trúc Medallion (Bronze → Silver → Gold)

```
┌──────────────────────────────────────────────────────────────────────┐
│                        EXTRACT (Apache Airflow)                      │
│  Open-Meteo Forecast API        Open-Meteo Archive API               │
│  (52 locations / batch call)    (backfill_history.py)             │
└────────────────┬────────────────────────┬────────────────────┬───────┘
                 ▼                        ▼                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER (Raw)                           │
│  api_openmeteo_raw_data          bronze_historical_weather           │
│  (JSONB, 1 row/batch)            (1 row/giờ/location, từ 2022)      │
│  UNIQUE(source_type, exec_date)  UNIQUE(datetime, lat, lon)          │
└────────────────────────────┬─────────────────────────────────────────┘
                             ▼ dbt LATERAL + UNNEST + UNION ALL
┌──────────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER (Cleaned)                        │
│  slv_weather_hourly              slv_air_quality_hourly              │
│  INCREMENTAL, DISTINCT ON        INCREMENTAL, DISTINCT ON            │
│  unique_key=(time, lat, lon)     unique_key=(time, lat, lon)         │
└────────────────────────────┬─────────────────────────────────────────┘
                             ▼ dbt LEFT JOIN + Derived Metrics
┌──────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Business-Ready)                        │
│           gold_layer.mart_hourly_conditions (TABLE)                  │
│    52 locations (30 HN + 22 HCM) × 168h forecast (~8,736 rows/run)     │
│    lấy dữ liệu độc lập cho toàn bộ các Quận/Huyện             │
│    location_name | forecast_time | weather | AQ | alert_flags        │
└──────────────────────────────────────────────────────────────────────┘
                             ▼
 ┌────────────────────────────────────────────────────────────┐
 │                  CONSUMPTION / SERVING LAYER               │
 │  Apache Superset Dashboard (Trực quan hóa)                 │
 │  Telegram Interactive Bot (Pull: Tra cứu /weather, /aqi)   │
 │  Alert Job Broadcast:                                       │
 │   ├─ 06:00 Bản tin Sáng: Quét toàn bộ ngày hôm nay          │
 │   ├─ 20:00 Bản tin Tối: Quét toàn bộ ngày mai               │
 │   └─ Khẩn cấp (giờ khác): Quét đột xuất 6 giờ tới           │
 └────────────────────────────────────────────────────────────┘

---

## Stack Công Nghệ

| Layer | Tool | Version | Vai Trò |
|---|---|---|---|
| Orchestration | Apache Airflow | 3.2.0 | DAG `@hourly`, task retry, idempotency |
| Extract | Python (OOP) | 3.13 | Batch API call, strict index matching (zip) |
| Storage | PostgreSQL | 16 | 3-tầng Bronze/Silver/Gold, JSONB, UNIQUE constraints |
| Transform | dbt-core | 1.9.0 | LATERAL unnest, DISTINCT ON dedup, LEFT JOIN |
| Infrastructure | Docker Compose | 2.x | Healthcheck, service dependency, DRY YAML anchors |
| Visualization | Apache Superset | 6.1.0 | 5-Container Architecture (App, Init, Worker, Beat, Redis) |
| Alerting & UI | pyTelegramBotAPI | 4.20.0 | Dual-Core Bot (Interactive Pull + Stateful Push), Max-Ping UX |

---

## Phạm Vi Dữ Liệu — 52 Khu Vực Quan Trắc

Danh sách 52 khu vực đã được thiết lập sẵn trong `config.json`. Mọi tọa độ đều được trích xuất từ Nominatim (OpenStreetMap) đại diện cho trung tâm các Quận, Huyện, và Thị xã.

---

## Luồng Xử Lý Dữ Liệu Chi Tiết

### Pipeline Realtime (Chạy Mỗi Giờ)

```
Airflow logical_date ──► main.py --execution_date {logical_date}
    │
    ├─► OpenMeteoExtractor.get_open_meteo_data(52 locations batch)
    │       └─► _inject_location_metadata()
    │               strict index matching (zip)
    │               inject: requested_lat, requested_lon, location_name
    │
    ├─► PostgresLoader.insert_data(weather_forecast_hourly)
    │       UPSERT → api_openmeteo_raw_data
    │       ON CONFLICT (source_type, execution_date) DO UPDATE
    │
    └─► PostgresLoader.insert_data(air_quality_hourly)
            UPSERT → api_openmeteo_raw_data

    ↓ (sau khi ELT thành công)
    dbt run  ──► 4 Staging VIEWs
              ──► 2 Silver INCREMENTAL (DISTINCT ON, execution_date == var)
              ──► 1 Gold TABLE (LEFT JOIN, derived metrics)
    dbt test ──► 29 data quality tests (PASS/FAIL)

    ↓ (Consumption Layer)
    alert_job.py    ─► Push: Stateful cronjob báo cáo sáng/tối & Cảnh báo khẩn
    telegram_bot.py ─► Pull: Interactive Bot phục vụ truy vấn On-demand của user
```

### Backfill Pipeline (Chạy 1 lần)

```bash
# Backfill toàn bộ dữ liệu lịch sử bằng API (100% tự động, không dùng CSV)
python3 backfill_history.py --location-prefix HN --start-date 2022-08-02 --end-date <YYYY-MM-DD>
python3 backfill_history.py --location-prefix HCM --start-date 2022-08-02 --end-date <YYYY-MM-DD>
```

---

## Cột Quan Trọng trong Gold Layer

| Cột | Kiểu | Mô Tả |
|---|---|---|
| `forecast_time` | TIMESTAMPTZ | Thời điểm dự báo (UTC) |
| `location_name` | VARCHAR | Tên quận/huyện (HN/HCM prefix) |
| `latitude`, `longitude` | NUMERIC | Toạ độ canonical từ config |
| `temperature_2m` | NUMERIC | Nhiệt độ °C |
| `pm2_5`, `pm10` | NUMERIC | Nồng độ hạt mịn µg/m³ |
| `temperature_level` | VARCHAR | Mát mẻ/Dễ chịu/Nóng/Rất nóng/Nguy hiểm |
| `uv_level` | VARCHAR | Thấp/Trung bình/Cao/Rất cao/Cực kỳ nguy hiểm |
| `pm2_5_level` | VARCHAR | Tốt/Trung bình/.../Nguy hiểm (AQI Mỹ) |
| `is_weather_alert` | BOOLEAN | TRUE khi temp≥38°C HOẶC UV≥8. KHÔNG NULL. |
| `is_air_quality_alert` | BOOLEAN\|NULL | TRUE khi PM2.5≥55. NULL = chưa có dữ liệu |
| `execution_date` | TIMESTAMPTZ | Airflow logical_date — dùng cho lineage |

---

## Quick Start

```bash
# 1. Tạo .env (xem .env.example)
cp .env.example .env

# 2. Khởi động (Postgres healthcheck trước khi Airflow start)
docker compose up -d --build

# 3. Đợi ~60s → Airflow UI: http://localhost:8080

# 4. Backfill dữ liệu lịch sử bằng Archive API (Hoàn toàn tự động)
docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py \
    --location-prefix HN --start-date 2022-08-02 --end-date 2026-05-27
    
docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py \
    --location-prefix HCM --start-date 2022-08-02 --end-date 2026-05-27

# 5. Build Silver + Gold thủ công
docker exec airflow_container bash -c \
    "dbt run  --full-refresh \
               --project-dir /opt/airflow/dbt-transform \
               --profiles-dir /home/airflow/.dbt && \
     dbt test --project-dir /opt/airflow/dbt-transform \
               --profiles-dir /home/airflow/.dbt"
```

>  **Reset hoàn toàn (xóa sạch data):**
> ```bash
> docker compose down -v && docker compose up -d --build
> ```

---

## Nguyên Tắc Kiến Trúc

| Nguyên Tắc | Cách Thực Hiện |
|---|---|
| **Idempotency** | UPSERT everywhere — chạy lại cùng pipeline không tạo duplicate |
| **Fail-Fast** | `raise` (không `return`) → Airflow đánh dấu FAILED đúng |
| **Coordinate Matching** | Strict Index Matching (zip) — an toàn tuyệt đối với lỗi grid snapping |
| **Time Alignment** | AQ/Weather align theo `time_str` dict key — không phải array position |
| **Timezone Safety** | `AT TIME ZONE 'Asia/Bangkok'` explicit ở Python và SQL |
| **NULL Safety** | Guard `IS NULL` tường minh trước mọi so sánh số trong CASE |
| **UNION ALL Safety** | Explicit column list (đúng thứ tự) trong cả hai bên UNION ALL |
| **Zero Hardcode** | Configurations nằm toàn bộ trong JSON (Hybrid: Fail-Fast cho cấu trúc, Resilient cho mạng) |
| **Data Contract** | Validate `expected_keys` trước khi load — không load JSON thiếu field |
| **RBAC Ready** | `gold_layer` schema riêng, phân quyền dễ dàng qua PostgreSQL GRANT |

---

## Kiến trúc Dual-Core Telegram Bot
Hệ thống sử dụng Bot Telegram làm giao diện người dùng cuối (Consumption Layer) với các chuẩn mực khắt khe:
- **Strict OOP**: Tách biệt hoàn toàn Controller (`telegram_bot.py`), Service/Formatter (`bot_services.py`), và DB Manager.
- **Zero Hardcode**: Toàn bộ mốc cảnh báo (VD: Mưa >2.0mm, PM2.5 >55), danh sách Quận, và Prefix khu vực được khóa trong `config_runtime_constant.json` và nạp qua Singleton `ConfigManager`.
- **Max-Ping UX**: Thuật ngữ nhân tính hóa, xử lý triệt để "0.0mm Paradox" (không mưa thì ẩn xác suất gây nhiễu), layout căn lề chuẩn Micro-Dashboard.
- **Bản tin Sáng (06:00 - AQI)**: Cảnh báo ô nhiễm không khí (bụi mịn PM2.5) cho **toàn bộ ngày hôm nay**, giúp người dùng chuẩn bị khẩu trang trước khi đi làm.
- **Bản tin Tối (20:00 - Mưa Lớn)**: Tổng hợp rủi ro mưa lớn cho **toàn bộ ngày mai**, giúp người dùng lên kế hoạch lịch trình.
- **Cảnh báo Đột xuất (Mưa Khẩn Cấp)**: Các giờ còn lại, `alert_job.py` liên tục quét trước **cửa sổ 6 giờ tới (tính từ thời điểm hiện tại)** để phát hiện mưa bất chợt. Tích hợp cơ chế **Stateful Deduplication** (ghi nhận trạng thái vào `silver_layer.alert_history`), đảm bảo cảnh báo khẩn cấp chỉ kích hoạt **đúng 1 lần** cho 1 cơn mưa, chống spam tuyệt đối.

---

## Cấu Trúc Thư Mục

```
weather-meteo-data-platform/
├── docker-compose.yml          # Infra: Postgres (healthcheck) + Airflow
├── Dockerfile                  # Airflow + dbt-postgres
├── .env                        # không commit Git
│
├── src/
│   ├── config/
│   │   ├── config.json                  # 52 locations + API URLs
│   │   └── config_runtime_constant.json # Cấu hình UI/UX cho Bot (Zero hardcode)
│   ├── extractors/open_meteo.py
│   ├── loaders/
│   │   ├── base_loader.py      # Connection pooling (OOP)
│   │   ├── postgres_loader.py  # UPSERT realtime data
│
│   ├── scripts/
│   │   ├── init_dbs.sh         # Tạo tables + UNIQUE constraints
│   │   ├── backfill_history.py # Tự động fetch lịch sử
│   │   ├── telegram_bot.py     # Lõi Pull: Giao tiếp tương tác qua Telegram
│   │   ├── alert_job.py        # Lõi Push: Phát thanh & Stateful Deduplication
│   │   └── bot_services.py     # Centralized formatting & UX logic
│   ├── utils/
│   │   ├── config_manager.py   # Singleton config loader (Hybrid Fail-Fast/Resilient)
│   │   └── logger.py
│   └── main.py                 # ELT entrypoint (--execution_date)
│
├── airflow/                     
│   ├── README.md
│   └── dags/orchestrator.py        # DAG @hourly, 4 tasks, retries=3
│
└── dbt-transform/
    ├── models/
    │   ├── staging/            # VIEW: flatten JSON, timezone shift
    │   ├── silver/             # INCREMENTAL: dedup + union historical
    │   └── marts/              # TABLE (gold_layer): joined flat mart
    └── macros/
```

---

## Data Quality Gates (29 dbt Tests)

| Test | Coverage |
|---|---|
| `not_null` | forecast_time, lat, lon, temperature_2m, is_weather_alert, location_name* |
| `accepted_values` | temperature_level (5 values), uv_level (6 values), pm2_5_level (6 values) |
| `source not_null` | Bronze: id, source_type, execution_date, datetime, lat, lon |
| `source unique` | Bronze: id trong cả 2 bảng |



---

## 📚 Tài Liệu Chi Tiết

| | |
|---|---|
| [src/README.md](./src/README.md) | Extract & Load Layer |
| [airflow/README.md](./airflow/README.md) | DAG, schedule, Airflow auth |
| [dbt-transform/README.md](./dbt-transform/README.md) | Models, materializations, tests |
| [RUNBOOK.md](./RUNBOOK.md) | Hướng dẫn vận hành và Setup Database cho Superset |
| [docs/superset_visualization_guide.md](./docs/superset_visualization_guide.md) | Bí kíp vẽ biểu đồ & 3 Lớp Phòng thủ Timezone |

---

## API Data Completeness
Dữ liệu được khai thác độc lập cho **52 khu vực (30 HN + 22 HCM)** với độ chính xác cao nhất từ Open-Meteo API. Pipeline đã loại bỏ toàn bộ dữ liệu CSV rác và lỗi thời để chuyển sang 100% tự động hóa.

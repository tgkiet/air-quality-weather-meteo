# ☁️ Vietnam Weather & Air Quality Data Platform ☁️

> **Enterprise-grade Data Engineering pipeline** tự động thu thập, chuẩn hóa và phân tích dữ liệu **thời tiết & chất lượng không khí** cho **20 khu vực quan trắc** trên 2 thành phố — **10 grid cells Hà Nội** và **10 grid cells TP.HCM** — cập nhật mỗi giờ và lưu trữ lịch sử từ năm 2022.
>
>  **Về Grid Resolution:** config.json ban đầu có 53 locations (31 HN + 22 HCM). Sau khi phân tích thực tế, Open-Meteo API chỉ trả về **20 grid cells độc lập** (resolution ~1km — các quận nội thành gần nhau được merge). Config đã được prune xuống 20 locations để loại bỏ duplicate và API call lãng phí.

---

## Kiến Trúc Medallion (Bronze → Silver → Gold)

```
┌──────────────────────────────────────────────────────────────────────┐
│                        EXTRACT (Apache Airflow)                      │
│  Open-Meteo Forecast API        Open-Meteo Archive API    CSV Files  │
│  (20 locations / batch call)    (backfill_history.py) (Hà Nội)  │
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
│    20 locations (10 HN + 10 HCM) × 168h forecast (~3,360 rows/run)     │
│    sau khi prune config khớp với API grid resolution (~1km)             │
│    location_name | forecast_time | weather | AQ | alert_flags        │
└──────────────────────────────────────────────────────────────────────┘
                             ▼
                    Apache Superset Dashboard
```

---

## Stack Công Nghệ

| Layer | Tool | Version | Vai Trò |
|---|---|---|---|
| Orchestration | Apache Airflow | 3.2.0 | DAG `@hourly`, task retry, idempotency |
| Extract | Python (OOP) | 3.13 | Batch API call, nearest-neighbor location matching |
| Storage | PostgreSQL | 16 | 3-tầng Bronze/Silver/Gold, JSONB, UNIQUE constraints |
| Transform | dbt-core | 1.9.0 | LATERAL unnest, DISTINCT ON dedup, LEFT JOIN |
| Infrastructure | Docker Compose | 2.x | Healthcheck, service dependency, DRY YAML anchors |
| Visualization | Apache Superset | 6.1.0 | 5-Container Architecture (App, Init, Worker, Beat, Redis) |

---

## Phạm Vi Dữ Liệu — 20 Khu Vực Quan Trắc

### Hà Nội — 10 Grid Cells

| Tên | Toạ Độ |
|---|---|
| HN Bắc Từ Liêm | 21.1105°N, 105.7605°E |
| HN Bắc Từ Liêm Tây | 21.05°N, 105.74°E |
| HN Chương Mỹ | 20.92°N, 105.7123°E |
| HN Cầu Giấy | 21.0478°N, 105.8°E |
| HN Hoàng Mai | 20.9883°N, 105.8549°E |
| HN Hoàng Mai Nam | 20.9481°N, 105.8493°E |
| HN Hà Đông Đông | 20.972°N, 105.7856°E |
| HN Long Biên | 21.0491°N, 105.8831°E |
| HN Nam Từ Liêm Tây | 21.0024°N, 105.7181°E |
| HN Đông Anh | 21.1476°N, 105.9159°E |

### TP.HCM — 10 Grid Cells

| Tên | Toạ Độ |
|---|---|
| HCM Huyện Cần Giờ | 10.3966°N, 106.9087°E |
| HCM Huyện Củ Chi | 11.0967°N, 106.5097°E |
| HCM Huyện Hóc Môn | 10.8777°N, 106.5761°E |
| HCM Huyện Nhà Bè | 10.651°N, 106.7259°E |
| HCM Quận 1 | 10.7754°N, 106.6996°E |
| HCM Quận 12 | 10.8613°N, 106.6642°E |
| HCM Quận 7 | 10.7378°N, 106.7297°E |
| HCM Quận 8 | 10.7211°N, 106.6292°E |
| HCM Quận Tân Phú | 10.7919°N, 106.6278°E |
| HCM Thành phố Thủ Đức | 10.851°N, 106.7549°E |

---

## Luồng Xử Lý Dữ Liệu Chi Tiết

### Pipeline Realtime (Chạy Mỗi Giờ)

```
Airflow logical_date ──► main.py --execution_date {logical_date}
    │
    ├─► OpenMeteoExtractor.get_open_meteo_data(20 locations batch)
    │       └─► _inject_location_metadata()
    │               nearest-neighbor matching (tolerance 0.15°)
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
              ──► 2 Silver INCREMENTAL (DISTINCT ON, execution_date >)
              ──► 1 Gold TABLE (LEFT JOIN, derived metrics)
    dbt test ──► 29 data quality tests (PASS/FAIL)
```

### Backfill Pipeline (Chạy 1 lần)

```bash
# HCM — Archive API (10 grid cells từ 2022 đến nay)
python3 backfill_history.py --location-prefix HCM \
    --start-date 2022-08-02 --end-date 2026-05-24
    └─► Lấy Weather + AQ theo từng location
    └─► Align AQ/Weather theo time key (dict lookup, không phải positional)
    └─► UPSERT → bronze_historical_weather
        ON CONFLICT (datetime, lat, lon) DO UPDATE

# Hà Nội — CSV (historical) + API gap
python3 load_historical_csvs.py         # CSV đến 2025-11-29
python3 backfill_history.py --location-prefix HN \
    --start-date 2025-11-30 --end-date 2026-05-24   # gap fill
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

# 4. Nạp lịch sử Hà Nội từ CSV
docker exec airflow_container \
    python3 /opt/airflow/src/scripts/load_historical_csvs.py

# 5. Backfill lịch sử HCM từ Archive API (~30-60 phút)
docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py \
    --location-prefix HCM \
    --start-date 2022-08-02 --end-date 2026-05-24

docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py \
    --location-prefix HN \
    --start-date 2025-11-30 --end-date 2026-05-24   # gap fill

# 6. Build Silver + Gold thủ công
docker exec airflow_container bash -c \
    "dbt run  --full-refresh \
               --project-dir /opt/airflow/dbt-transform \
               --profiles-dir /home/airflow/.dbt && \
     dbt test --project-dir /opt/airflow/dbt-transform \
               --profiles-dir /home/airflow/.dbt"
```

> ⚠️ **Reset hoàn toàn (xóa sạch data):**
> ```bash
> docker compose down -v && docker compose up -d --build
> ```

---

## Nguyên Tắc Kiến Trúc

| Nguyên Tắc | Cách Thực Hiện |
|---|---|
| **Idempotency** | UPSERT everywhere — chạy lại cùng pipeline không tạo duplicate |
| **Fail-Fast** | `raise` (không `return`) → Airflow đánh dấu FAILED đúng |
| **Coordinate Matching** | Nearest-neighbor (tolerance 0.15°) — không dùng positional index |
| **Time Alignment** | AQ/Weather align theo `time_str` dict key — không phải array position |
| **Timezone Safety** | `AT TIME ZONE 'Asia/Bangkok'` explicit ở Python và SQL |
| **NULL Safety** | Guard `IS NULL` tường minh trước mọi so sánh số trong CASE |
| **UNION ALL Safety** | Explicit column list (đúng thứ tự) trong cả hai bên UNION ALL |
| **No Hardcoding** | Locations → `config.json`, credentials → `.env` |
| **Data Contract** | Validate `expected_keys` trước khi load — không load JSON thiếu field |
| **RBAC Ready** | `gold_layer` schema riêng, phân quyền dễ dàng qua PostgreSQL GRANT |

---

## Cấu Trúc Thư Mục

```
weather-meteo-data-platform/
├── docker-compose.yml          # Infra: Postgres (healthcheck) + Airflow
├── Dockerfile                  # Airflow + dbt-postgres
├── .env                        # không commit Git
│
├── src/
│   ├── config/config.json      # 20 locations + API URLs/params
│   ├── extractors/open_meteo.py
│   ├── loaders/
│   │   ├── base_loader.py      # Connection pooling
│   │   ├── postgres_loader.py  # UPSERT realtime data
│   │   └── csv_loader.py       # COPY + UPSERT historical CSV
│   ├── scripts/
│   │   ├── init_dbs.sh         # Tạo tables + UNIQUE constraints
│   │   ├── load_historical_csvs.py
│   │   └── backfill_history.py  # --start-date, --end-date
│   ├── utils/logger.py
│   └── main.py                 # ELT entrypoint (--execution_date)
│
├── airflow/dags/orchestrator.py   # DAG: @hourly, 3 tasks, retries=3
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
| `not_null (warn)` | location_name trong Silver — warn vì Hanoi CSV historical có thể NULL |

> *`location_name` test ở Gold dùng `severity: warn` để không block pipeline khi có Hanoi historical data (NULL từ CSV cũ).

---

## Tài Liệu Chi Tiết

| | |
|---|---|
| [src/README.md](./src/README.md) | Extract & Load Layer |
| [airflow/README.md](./airflow/README.md) | DAG, schedule, Airflow auth |
| [dbt-transform/README.md](./dbt-transform/README.md) | Models, materializations, tests |

---

## API Grid Resolution

Open-Meteo Forecast/Archive API snap toạ độ về lưới model riêng (~1km resolution). Khi nhiều locations trong config có toạ độ cách nhau <0.1° (thường gặp ở các quận nội thành), API tự động merge chúng về cùng 1 điểm lưới và chỉ trả về 1 response item.

### Ảnh Hưởng Thực Tế

| Metric | Config | Thực tế API |
|---|---|---|
| HN locations | 31 | ~10 grid cells |
| HCM locations | 22 | ~10 grid cells |
| **Tổng** | **53** | **~20 khu vực đại diện** |

### Cách Pipeline Xử Lý

1. **`_inject_location_metadata()`** trong `main.py`: Với mỗi API response item, dùng **nearest-neighbor matching** (tolerance 0.15°) để tìm config location gần nhất và gán `location_name`. Không có response item nào bị "UNKNOWN".

2. **Kết quả trong Silver/Gold**: Mỗi "khu vực đại diện" (grid cell) chứa dữ liệu thời tiết đại diện cho **tất cả các quận nằm trong bán kính ~5km** xung quanh đó. Đây là giới hạn của API công khai miễn phí.

3. **Không mất dữ liệu, chỉ giảm độ phân giải không gian**: Các quận cách nhau <1km (như HCM Quận 5 và Quận 10) sẽ có cùng chỉ số thời tiết — đây là hành vi đúng và được kỳ vọng.

### Nếu Cần Độ Phân Giải Cao Hơn

Xem xét Open-Meteo **Commercial API** hoặc nguồn dữ liệu khác (VD: VNMHA, WeatherAPI) có resolution cao hơn cho các điểm đô thị dày đặc.


# 🐍 src/ — Extract & Load Layer

> Toàn bộ logic Python cho bước **E (Extract)** và **L (Load)** trong chuỗi ELT. Tuân thủ **OOP**, **Twelve-Factor App**, **Separation of Concerns**, và **Idempotency**.

---

## Cấu Trúc Module

```
src/
├── main.py                          # Entrypoint — điều phối Extract & Load
│
├── config/
│   ├── config.json                  # 53 locations + API URLs/params
│   └── config_runtime_constant.json # timeout, max_retries, retry_delay
│
├── extractors/
│   └── open_meteo.py                # OpenMeteoExtractor (Session, Retry, Contract)
│
├── loaders/
│   ├── base_loader.py               # BasePostgresLoader (connect, close, retry)
│   ├── postgres_loader.py           # UPSERT realtime data → Bronze
│   └── csv_loader.py                # COPY + UPSERT historical CSV → Bronze
│
├── scripts/
│   ├── init_dbs.sh                  # Khởi tạo DB, tables, UNIQUE constraints
│   ├── load_historical_csvs.py      # Nạp CSV Hà Nội → bronze_historical_weather
│   └── backfill_history.py      # Backfill HCM từ Archive API (argparse)
│
└── utils/
    ├── config_manager.py            # ConfigManager Singleton
    └── logger.py                    # Console (Docker) / File (local)
```

---

## main.py — ELT Entrypoint

Nhận `--execution_date` từ Airflow BashOperator (Jinja template `{{ logical_date | ts }}`). **Tuyệt đối không** dùng `datetime.now()` — vi phạm Idempotency.

```bash
# Chạy thủ công (test)
python src/main.py --execution_date "2026-05-21T04:00:00+00:00"
```

**Luồng xử lý:**
1. Load config → build batch params (53 lats, 53 lons joined bằng dấu phẩy)
2. Extract Weather API → `_inject_location_metadata()` → nearest-neighbor match
3. Extract AQ API → `_inject_location_metadata()`
4. UPSERT cả 2 vào `api_openmeteo_raw_data` (Bronze)

---

## extractors/open_meteo.py

| Feature | Chi Tiết |
|---|---|
| **Session Pooling** | `requests.Session` tái dùng TCP — tránh handshake mỗi request |
| **Exponential Backoff** | Retry 3 lần: 0s → 2s → 4s. Chỉ retry 429/5xx, không retry 4xx |
| **Data Contract** | `get_open_meteo_data(params, expected_keys)` — validate schema trước khi return |
| **Fail-Fast** | `raise` nếu contract fail → Airflow nhận FAILED, không load rác |

---

## loaders/

### base_loader.py
- `BasePostgresLoader` — base class cho tất cả loaders
- `connect()` với retry 3 lần, dùng biến môi trường từ `.env`
- `close()` — đảm bảo không rò rỉ connection (gọi trong `finally`)

### postgres_loader.py
- UPSERT vào `api_openmeteo_raw_data` với `ON CONFLICT (source_type, execution_date) DO UPDATE`
- Tên bảng dùng `psycopg2.sql.Identifier` → tránh SQL Injection
- Serialize `raw_json` với `json.dumps()` → lưu nguyên vẹn JSONB

### csv_loader.py
- **COPY** từ CSV → TEMP TABLE (nhanh hơn INSERT thông thường 10-20×)
- UPSERT từ TEMP TABLE → `bronze_historical_weather`
- Tự động thêm UNIQUE constraint nếu chưa có (idempotent)

---

## scripts/

### init_dbs.sh
Chạy tự động khi Postgres container khởi tạo lần đầu. Tạo:
- `api_openmeteo_raw_data` + `UNIQUE(source_type, execution_date)`
- `bronze_historical_weather` + `UNIQUE(datetime, lat, lon)` + cột `location_name`

### backfill_history.py
```bash
# Nhận ngày qua argparse (không hardcode)
python3 backfill_history.py --start-date 2022-08-02 --end-date 2026-05-19
```
- Filter locations có prefix `"HCM "` từ `config.json`
- Gọi Archive API riêng biệt cho Weather + AQ
- **Align AQ/Weather theo `time_str` dict key** (không phải positional index)
- UPSERT vào `bronze_historical_weather`

### load_historical_csvs.py
- Tự động detect đường dẫn CSV theo 3 mức ưu tiên:
  1. `$CSV_DATA_DIR/filename` (env var, dùng cho Docker volume mount)
  2. `src/data/filename` (trong project — khuyến nghị cho production)
  3. `../Open-Meteo-Dataset/filename` (host path, chỉ dùng khi dev)
- `hanoi_aq_weather_MERGED.csv` là **bắt buộc** — script fail nếu không tìm thấy
- `hanoi_realtime_data_updated.csv` là **optional** — bỏ qua nếu không có

---

## Dữ Liệu Thu Thập

### Thời Tiết (`weather_forecast_hourly`)
| Biến | Đơn Vị | Mô Tả |
|---|---|---|
| `temperature_2m` | °C | Nhiệt độ tại 2m |
| `relative_humidity_2m` | % | Độ ẩm |
| `dew_point_2m` | °C | Điểm sương |
| `apparent_temperature` | °C | Nhiệt độ cảm nhận |
| `precipitation_probability` | % | Xác suất có mưa |
| `precipitation` | mm | Lượng mưa |
| `pressure_msl` | hPa | Áp suất khí quyển |
| `cloud_cover` | % | Độ phủ mây |
| `visibility` | m | Tầm nhìn |
| `wind_speed_10m` | km/h | Tốc độ gió |
| `wind_direction_10m` | ° | Hướng gió |
| `wind_gusts_10m` | km/h | Gió giật |
| `uv_index` | — | Chỉ số UV |

### Chất Lượng Không Khí (`air_quality_hourly`)
| Biến | Đơn Vị | Mô Tả |
|---|---|---|
| `pm10` | µg/m³ | Bụi mịn PM10 |
| `pm2_5` | µg/m³ | Bụi siêu mịn PM2.5 |
| `carbon_monoxide` | µg/m³ | CO |
| `nitrogen_dioxide` | µg/m³ | NO₂ |
| `sulphur_dioxide` | µg/m³ | SO₂ |
| `ozone` | µg/m³ | Ozone tầng mặt đất |
| `aerosol_optical_depth` | — | Độ đục quang học |
| `dust` | µg/m³ | Bụi thô |
| `uv_index` | — | Chỉ số UV |
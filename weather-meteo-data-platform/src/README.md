# Source Code — Extract & Load Layer

Thư mục `src/` chứa toàn bộ logic Python thực hiện **Bước E (Extract)** và **Bước L (Load)** trong cấu trúc ELT. Dựa trên triết lý **Twelve-Factor App**, **Separation of Concerns**, và **Idempotency**.

---

## Cấu Trúc Module

```
src/
├── main.py                  # Entrypoint — điều phối Extract & Load
│
├── config/                  # Cấu hình ứng dụng → README: config/README.md
│   ├── config.json                  # URL & params API
│   └── config_runtime_constant.json # timeout, max_retries, delay
│
├── extractors/              # Lớp Extract → README: extractors/README.md
│   └── open_meteo.py        # OpenMeteoExtractor (Session, Retry, Data Contract)
│
├── loaders/                 # Lớp Load → README: loaders/README.md
│   └── postgres_loader.py   # PostgresLoader (UPSERT, Idempotency, Fail-fast)
│
├── scripts/                 # Khởi tạo hệ thống → README: scripts/README.md
│   └── init_dbs.sh          # Tạo DB, bảng Bronze, UNIQUE CONSTRAINT
│
└── utils/                   # Tiện ích dùng chung → README: utils/README.md
    ├── config_manager.py    # ConfigManager Singleton
    └── logger.py            # Logger chuẩn hóa
```

---

## Documentation Chi Tiết

| Module | Nội dung |
|---|---|
| [extractors/](./extractors/README.md) | Session Pooling, Exponential Backoff Retry, Data Contract Validation |
| [loaders/](./loaders/README.md) | Fail-fast, UPSERT vs DELETE+INSERT, MVCC, Idempotency |
| [utils/](./utils/README.md) | Singleton Pattern, Logger theo môi trường, Twelve-Factor Config |
| [config/](./config/README.md) | Phân biệt 2 loại config, khi nào sửa file nào |
| [scripts/](./scripts/README.md) | init_dbs.sh, schema Bronze Layer, tại sao cần UNIQUE CONSTRAINT |

---

## Các Tính Năng Enterprise-Grade

### 1. Tính Lũy Đẳng (Idempotency) ở Lớp Bronze
- Pipeline dùng **UPSERT** (`INSERT ... ON CONFLICT DO UPDATE`) thay vì DELETE+INSERT
- `execution_date` = Airflow `logical_date` (không phải `datetime.now()`) làm Natural Key
- Chạy 1 lần hay 10 lần cho cùng 1 batch → DB luôn chỉ có 1 dòng duy nhất

### 2. Data Contract Validation (Trạm kiểm duyệt schema)
- `OpenMeteoExtractor.get_open_meteo_data(params, expected_keys)` nhận Data Contract từ caller
- Chặn mọi response JSON không đúng schema (kể cả HTTP 200 với nội dung bảo trì)
- **Dependency Injection**: caller tự định nghĩa contract → class Extractor tái dụng được

### 3. Connection Pooling & Smart Retry
- `requests.Session` tái dụng TCP connection (tránh Handshake mỗi request)
- `urllib3.Retry` với Exponential Backoff: 0s → 2s → 4s
- Chỉ retry HTTP 429/5xx, không retry lỗi 4xx (lỗi client)

### 4. Twelve-Factor App Configuration
- Magic Numbers (`timeout`, `max_retries`) ra file JSON, không hardcode trong code
- `ConfigManager` Singleton đọc file JSON đúng 1 lần cho toàn bộ vòng đời ứng dụng

---

## Chạy Thử Từ Terminal

```bash
# Từ thư mục weather-meteo-data-platform/
python src/main.py --execution_date "2026-05-06T10:00:00+00:00"
```

`--execution_date` bắt buộc phải truyền vào. Trong Production, Airflow tự truyền qua Jinja Template:
```python
bash_command='python3 /opt/airflow/src/main.py --execution_date "{{ logical_date | ts }}"'
```

---

## Dữ Liệu Thu Thập

### Thời Tiết — `source_type: weather_forecast_hourly`
| Biến | Đơn Vị | Mô Tả |
|---|---|---|
| `temperature_2m` | °C | Nhiệt độ tại 2m |
| `relative_humidity_2m` | % | Độ ẩm tương đối |
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

### Chất Lượng Không Khí — `source_type: air_quality_hourly`
| Biến | Đơn Vị | Mô Tả |
|---|---|---|
| `pm10` | μg/m³ | Bụi mịn PM10 |
| `pm2_5` | μg/m³ | Bụi siêu mịn PM2.5 |
| `carbon_monoxide` | μg/m³ | CO |
| `nitrogen_dioxide` | μg/m³ | NO₂ |
| `sulphur_dioxide` | μg/m³ | SO₂ |
| `ozone` | μg/m³ | Ozone tầng mặt đất |
| `aerosol_optical_depth` | — | Độ đục quang học |
| `dust` | μg/m³ | Bụi thô |
| `uv_index` | — | Chỉ số UV |

# Tiến hành Backfill dữ liệu cũ đã crawl thành file csv lúc trước
- Load toàn bộ dữ liệu từ file csv vào DB
- Sử dụng dbt để transform dữ liệu từ Bronze lên Silver và Gold
- Dữ liệu được lưu trữ ở định dạng Parquet (dùng Pandas để đọc và lưu)
- Sử dụng dbt để transform dữ liệu từ Bronze lên Silver và Gold
- Lưu ý: về lựa chọn chiến lược để làm, kiến trúc để backfill
# 🐍 Source Code — Extract & Load Layer

Thư mục `src/` chứa toàn bộ logic Python thực hiện **Bước E (Extract)** và **Bước L (Load)** trong mô hình ELT. Code được viết theo chuẩn **OOP** và **Separation of Concerns** — mỗi class đảm nhận đúng một trách nhiệm duy nhất.

---

## Cấu Trúc Module

```
src/
├── main.py                  # Entrypoint — "dán keo" Extract và Load lại với nhau
│
├── config/
│   └── config.json          # Cấu hình API (URL, tọa độ, các biến dữ liệu)
│
├── extractors/
│   └── open_meteo.py        # Class OpenMeteoExtractor
│
├── loaders/
│   └── postgres_loader.py   # Class PostgresLoader
│
├── scripts/
│   └── init_dbs.sh          # Bash script tạo Database khi Postgres khởi động
│
└── utils/
    ├── config_manager.py    # Singleton ConfigManager (đọc config.json)
    └── logger.py            # Logger chuẩn hóa
```

---

## Mô Tả Chi Tiết Từng Module

### `config/config.json` — Nguồn Cấu Hình Duy Nhất

File JSON là nguồn sự thật duy nhất (Single Source of Truth) cho tất cả cấu hình API. Mọi thay đổi về endpoint, tọa độ địa lý, hoặc các biến dữ liệu cần thu thập đều thực hiện **tại đây, không ở đâu khác**.

```json
{
  "api": {
    "open_meteo": {
      "weather_url": "https://api.open-meteo.com/v1/forecast",
      "weather_params": { "latitude": 10.7756, "longitude": 106.7019, ... },
      "aq_url": "https://air-quality-api.open-meteo.com/v1/air-quality",
      "aq_params": { "latitude": 10.7756, "longitude": 106.7019, ... }
    }
  }
}
```

---

### `utils/config_manager.py` — ConfigManager (Singleton Pattern)

Triển khai **Singleton Design Pattern** để đảm bảo `config.json` chỉ được đọc từ disk đúng một lần duy nhất trong vòng đời của ứng dụng.

```python
from src.utils.config_manager import ConfigManager

config = ConfigManager().get_config()
weather_url = config["api"]["open_meteo"]["weather_url"]
```

**Tại sao Singleton?** Nếu nhiều module cùng khởi tạo `ConfigManager`, không cần lo tốn tài nguyên đọc file nhiều lần — chúng đều dùng chung một instance duy nhất trong bộ nhớ.

---

### `extractors/open_meteo.py` — OpenMeteoExtractor

Class này chịu trách nhiệm **duy nhất**: gọi HTTP GET đến Open-Meteo API và trả về JSON thô.

```python
extractor = OpenMeteoExtractor(url="https://api.open-meteo.com/v1/forecast")
data = extractor.get_open_meteo_data(params={...})
```

**Cơ chế Retry:** Tự động retry tối đa **3 lần** với delay **5 giây** nếu gặp lỗi mạng hoặc server trả về HTTP error. Nếu sau 3 lần vẫn thất bại, `raise Exception` để Airflow nhận biết và đánh dấu task FAILED.

---

### `loaders/postgres_loader.py` — PostgresLoader

Class này chịu trách nhiệm **duy nhất**: kết nối PostgreSQL và ghi dữ liệu vào bảng.

```python
loader = PostgresLoader()
loader.connect()
loader.insert_data(
    table_name="api_openmeteo_raw_data",
    source_type="weather_forecast_hourly",
    raw_json=data
)
loader.close()
```

**Bảo mật SQL Injection:** Tên bảng được truyền qua `psycopg2.sql.Identifier` — không bao giờ nối chuỗi SQL thủ công (`f"INSERT INTO {table_name}"`). Đây là lỗ hổng phổ biến mà nhiều developer mắc phải.

**Cấu hình kết nối** được đọc hoàn toàn từ biến môi trường (`.env`). Trong Docker, `POSTGRES_HOST=postgres_db` và `POSTGRES_PORT=5432` được inject tự động qua `docker-compose.yml`.

---

### `scripts/init_dbs.sh` — Database Initialization

Script bash này được mount vào thư mục `/docker-entrypoint-initdb.d/` của PostgreSQL container. Postgres sẽ tự động thực thi nó **một lần duy nhất** khi volume data chưa tồn tại (lần đầu khởi động hoặc sau khi `docker compose down -v`).

Script thực hiện:
1. Tạo Database `air_quality_db` với user và password từ biến môi trường
2. Tạo Database `airflow_db` với user riêng biệt cho Airflow

**Tại sao 2 Database?**
- `air_quality_db` = Data Warehouse của bạn (dữ liệu thật)
- `airflow_db` = Metadata của Airflow (lịch sử chạy DAG, logs...)
- Tách riêng để dễ backup, restore, và bảo mật độc lập

---

### `utils/logger.py` — Logger Chuẩn Hóa

Cung cấp hàm `get_logger(name)` trả về một logger đã được cấu hình:
- **Console output**: Hiển thị real-time khi chạy
- **File output**: Ghi vào `logs/pipeline.log` để tra cứu sau

```python
from src.utils.logger import get_logger
logger = get_logger("TenModule")

logger.info("Pipeline đang chạy...")
logger.error("Đã xảy ra lỗi!")
```

---

### `main.py` — Entrypoint

File này là điểm nối giữa Extract và Load. Nó không chứa bất kỳ logic nghiệp vụ nào — chỉ đơn giản là gọi các class theo đúng thứ tự.

**Chạy thử không cần Airflow:**
```bash
# Từ thư mục weather-meteo-data-platform/
python src/main.py
```

---

## Dữ Liệu Thu Thập

### Thời Tiết (Weather) — `source_type: weather_forecast_hourly`
| Biến | Đơn Vị | Mô Tả |
|---|---|---|
| `temperature_2m` | °C | Nhiệt độ tại độ cao 2m |
| `relative_humidity_2m` | % | Độ ẩm tương đối |
| `dew_point_2m` | °C | Điểm sương |
| `apparent_temperature` | °C | Nhiệt độ cảm nhận (feel-like) |
| `precipitation_probability` | % | Xác suất có mưa |
| `precipitation` | mm | Lượng mưa |
| `pressure_msl` | hPa | Áp suất khí quyển mực nước biển |
| `cloud_cover` | % | Độ phủ mây |
| `visibility` | m | Tầm nhìn |
| `wind_speed_10m` | km/h | Tốc độ gió |
| `wind_direction_10m` | ° | Hướng gió |
| `wind_gusts_10m` | km/h | Gió giật |
| `uv_index` | — | Chỉ số tia UV |

### Chất Lượng Không Khí (Air Quality) — `source_type: air_quality_hourly`
| Biến | Đơn Vị | Mô Tả |
|---|---|---|
| `pm10` | μg/m³ | Bụi hạt mịn PM10 |
| `pm2_5` | μg/m³ | Bụi hạt siêu mịn PM2.5 |
| `carbon_monoxide` | μg/m³ | Khí CO (Carbon Monoxide) |
| `nitrogen_dioxide` | μg/m³ | Khí NO₂ (Nitrogen Dioxide) |
| `sulphur_dioxide` | μg/m³ | Khí SO₂ (Sulphur Dioxide) |
| `ozone` | μg/m³ | Ozone tầng mặt đất |
| `aerosol_optical_depth` | — | Độ đục quang học của không khí |
| `dust` | μg/m³ | Bụi thô |
| `uv_index` | — | Chỉ số tia UV |

# 🐍 Source Code — Extract & Load Layer

Thư mục `src/` chứa toàn bộ logic Python thực hiện **Bước E (Extract)** và **Bước L (Load)** trong cấu trúc ELT. Dựa trên triết lý **Twelve-Factor App**, **Separation of Concerns**, và **Idempotency**, lớp này được thiết kế để chịu tải cao, an toàn tuyệt đối và bảo trì dễ dàng.

---

## Cấu Trúc Module

```
src/
├── main.py                  # Entrypoint — kết nối và điều phối Extract & Load, hỗ trợ argparse
│
├── config/
│   ├── config.json                  # Cấu hình API endpoints (URL, parameters, tọa độ)
│   └── config_runtime_constant.json # Các hằng số vận hành (timeout, max_retries, delay)
│
├── extractors/
│   └── open_meteo.py        # Class OpenMeteoExtractor (Data Contract, Session Pooling)
│
├── loaders/
│   └── postgres_loader.py   # Class PostgresLoader (UPSERT MVCC Optimization)
│
├── scripts/
│   └── init_dbs.sh          # Khởi tạo Database Postgres bằng Bash khi khởi động container
│
└── utils/
    ├── config_manager.py    # Singleton ConfigManager nạp các file JSON cấu hình
    └── logger.py            # Logger chuẩn hóa xuất ra Console & File
```

---

## Các Tính Năng Enterprise-Grade (Nổi bật cho CV Data Engineer)

### 1. Tính Lũy Đẳng (Idempotency) ở Lớp Bronze
- Lớp `PostgresLoader` được thiết kế để đảm bảo không bao giờ sinh ra dữ liệu rác nếu Pipeline bị lỗi hoặc phải chạy bù (Catchup/Backfill).
- **Cơ chế UPSERT:** Thay vì dùng lệnh DELETE/INSERT (có thể sinh Dead Tuples làm phình DB theo chuẩn MVCC), mã nguồn sử dụng lệnh `INSERT ... ON CONFLICT DO UPDATE`. 
- PostgreSQL sẽ dựa trên Unique Constraint `(source_type, execution_date)` để cập nhật dữ liệu một cách tốn ít chi phí phần cứng nhất.

### 2. Trạm Kiểm Duyệt Schema (Data Contract Validation)
- Class `OpenMeteoExtractor` không mù quáng tin tưởng dữ liệu API. API có thể trả về thông báo bảo trì `{"message": "Maintenance"}` với HTTP 200 OK, gây sập toàn bộ hệ thống lưu trữ tĩnh.
- **Dependency Injection:** File `main.py` sẽ quy định Hợp đồng Dữ Liệu (`expected_keys={"latitude", "longitude", "hourly"}`) và "tiêm" (inject) vào Extractor.
- Extractor kiểm tra xem Response từ API có đáp ứng đủ Schema cam kết không. Nếu không, ngay lập tức chặn lại và báo `Data Contract Violation`.

### 3. Tối ưu Mạng và Kết Nối (Connection Pooling & Retry)
- **Session Pooling:** Sử dụng `requests.Session()` thay vì `requests.get()` để tạo kết nối TCP liên tục (Keep-Alive), tránh chi phí bắt tay Handshake liên tục khi gửi nhiều request.
- **Exponential Backoff:** Dùng thư viện `urllib3.util.retry.Retry` để tự động giãn cách thời gian khi Retry. Lần 1 chờ 0s, lần 2 chờ 2s, lần 3 chờ 4s để giảm tải cho Server bị ngập lụt request (mã lỗi 429).
- **psycopg2.extras.Json:** Delegated toàn bộ việc đóng gói JSONB cho C-driver của thư viện psycopg2 để tăng hiệu suất gấp nhiều lần.

### 4. Triết lý cấu hình tách biệt (12-Factor App Configuration)
- Các con số nhạy cảm (Magic Numbers) như `timeout=10` hay `max_retries=3` đều bị loại bỏ khỏi Code Logic Python.
- Chúng được đưa ra file `config_runtime_constant.json` thông qua `ConfigManager` (Singleton Pattern). Team DevOps có thể chỉnh sửa các thông số này không cần động tới mã nguồn.

---

## 🛠️ Mô Tả Chi Tiết Từng Module

### `config/` — Nguồn Sự Thật Duy Nhất (Single Source of Truth)
- `config.json`: Chứa các URL tĩnh, Endpoint, và parameters (VD: Tọa độ, múi giờ).
- `config_runtime_constant.json`: Chứa số lần thử lại mạng, giới hạn timeout.

### `utils/config_manager.py` — ConfigManager (Singleton)
Dùng chung 1 instance trong bộ nhớ thay vì đọc file JSON từ đĩa cứng nhiều lần ở nhiều file.

### `extractors/open_meteo.py`
Nhiệm vụ: Truyền request API, Retry thông minh nếu sập server, thực thi Data Contract.
- Có khả năng chặn bất kì Exception lặt vặt bằng cơ chế Catch-all Anti-Pattern để giữ lại Traceback gốc bằng cú pháp `raise ... from e`.

### `loaders/postgres_loader.py`
Nhiệm vụ: Tiêm dữ liệu vào PostgreSQL bằng Context Manager `with cursor` (ngăn Rò rỉ con trỏ Database).
- Triển khai **Fail-fast Validation** để cảnh báo ngay từ vòng ngoài nếu DevOps chưa cung cấp đủ các biến môi trường DB như `POSTGRES_USER`.

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

### `main.py` — Entrypoint (CLI Argument)
Script mồi dùng để dán (glue) Extractor và Loader.
- Khước từ việc sử dụng bẫy thời gian `datetime.now()` để gán nhãn `execution_date`. Thay vào đó, sử dụng module `argparse` để đón lấy Argument thời điểm chính xác từ BashOperator của Airflow truyền vào bằng cú pháp Jinja Template (`{{ logical_date | ts }}`).

---

## 🏃 Cách Chạy Thử Ngang Bằng Terminal
```bash
# Chạy với thời gian ảo (đóng giả việc Airflow tiêm Logical Date vào):
python src/main.py --execution_date "2026-05-06T10:00:00"
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

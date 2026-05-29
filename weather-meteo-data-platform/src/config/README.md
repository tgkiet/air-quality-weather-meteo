# `config/` - Cấu hình ứng dụng

## Nhiệm vụ

Thư mục này là nơi lưu các cấu hình không nhạy cảm của pipeline.

- Cấu hình API Open-Meteo và danh sách 52 khu vực quan trắc nằm trong `config.json`.
- Hằng số vận hành (retry, timeout, delay) và cấu hình Bot UI/UX (ngưỡng cảnh báo, danh sách quận hiển thị) nằm trong `config_runtime_constant.json`.
- Thông tin nhạy cảm như host/user/password database không đặt ở đây, mà quản lý qua `.env`.

## Cấu trúc thư mục

```text
config/
├── config.json
├── config_runtime_constant.json
└── README.md
```

## `config.json` - Cấu hình API

File này chứa endpoint và query parameters cho Open-Meteo. `src/main.py` đọc trực tiếp file này qua hàm `_load_api_config()`.

```json
{
  "locations": [
    { "name": "HCM Quận 1", "latitude": 10.775394, "longitude": 106.699625 },
    { "name": "HN Quận Ba Đình", "latitude": 21.034709, "longitude": 105.824519 }
    // ... 50 locations khác
  ],
  "api": {
    "open_meteo": {
      "weather_url": "https://api.open-meteo.com/v1/forecast",
      "weather_params": {
        "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "hourly": "temperature_2m,relative_humidity_2m,...,uv_index",
        "timezone": "Asia/Bangkok",
        "past_days": 3
      },
      "aq_url": "https://air-quality-api.open-meteo.com/v1/air-quality",
      "aq_params": {
        "hourly": "pm10,pm2_5,carbon_monoxide,...,uv_index",
        "timezone": "Asia/Bangkok",
        "past_days": 3
      }
    }
  }
}
```

### Khi nào sửa file này?

- Thay đổi tọa độ địa điểm cần lấy dữ liệu.
- Thêm hoặc bớt biến thời tiết/không khí trong `current` hoặc `hourly`.
- Thay đổi endpoint API.
- Thay đổi timezone truyền lên Open-Meteo.

## `config_runtime_constant.json` - Hằng số vận hành

File này chứa các thông số kỹ thuật có thể thay đổi theo môi trường. File được quản lý qua `ConfigManager` singleton trong `src/utils/config_manager.py`.

| Thông số | Giá trị | Ý nghĩa |
|---|---|---|
| `api.max_retries` | `3` | Số lần retry khi gọi API thất bại |
| `api.backoff_factor` | `1` | Hệ số giãn cách giữa các lần retry API |
| `api.timeout_sec` | `30` | Timeout cho mỗi request API, tính bằng giây |
| `database.max_retries` | `3` | Số lần retry khi kết nối PostgreSQL thất bại |
| `database.retry_delay_sec` | `5` | Thời gian chờ giữa các lần retry database, tính bằng giây |
| `alert_thresholds` | `Dict` | Các mốc cảnh báo (mưa > 2.0mm, tỉ lệ > 80%, PM2.5 > 55) dùng chung cho Bot |
| `telegram_bot.districts` | `List` | Danh sách các Quận/Huyện hiển thị lên Menu của Telegram Bot |
| `alert_job.target_region_prefix` | `"HCM "` | Dùng để lọc khu vực phát thanh cảnh báo khẩn cấp (Push Alert) |

### Khi nào sửa file này?

- API phản hồi chậm và cần tăng `timeout_sec`.
- API hoặc network không ổn định và cần tăng `max_retries`.
- Database khởi động chậm trong Docker/Airflow và cần tăng `retry_delay_sec`.
- Môi trường production cần ngưỡng timeout/retry khác môi trường local.
- Cần điều chỉnh **mốc cảnh báo mưa / không khí** cho Bot mà không phải sửa Code.
- Thêm hoặc bớt các Quận hiển thị trong Bot Telegram.

Nếu file này bị mất hoặc JSON sai cú pháp, `ConfigManager` sẽ fallback về giá trị mặc định trong code. Lưu ý giá trị fallback hiện tại của `api.timeout_sec` là `10`, khác với giá trị runtime đang cấu hình trong file JSON là `30`.

## Tại sao tách 2 file?

| Tiêu chí | `config.json` | `config_runtime_constant.json` |
|---|---|---|
| Bản chất | Cấu hình nghiệp vụ/API | Cấu hình kỹ thuật/runtime |
| Nội dung | Endpoint, tọa độ, danh sách biến cần lấy | Retry, timeout, delay, Bot thresholds, Bot UI |
| Module đọc | `src/main.py` | `src/utils/config_manager.py` |
| Module sử dụng | `OpenMeteoExtractor` nhận URL/params từ `main.py` | `OpenMeteoExtractor`, `PostgresLoader`, Bot, Alert Job |
| Người thường sửa | Data Engineer | DevOps/Platform Engineer, Product Manager |

Tách riêng như vậy giúp thay đổi logic lấy dữ liệu API mà không ảnh hưởng tới các thông số vận hành, và ngược lại.

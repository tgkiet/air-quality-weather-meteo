# config/ — Cấu Hình Ứng Dụng

## Nhiệm vụ
**Nguồn sự thật duy nhất (Single Source of Truth)** cho toàn bộ cấu hình ứng dụng. Thư mục này chứa cấu hình không nhạy cảm. Các thông tin nhạy cảm (mật khẩu DB) quản lý qua `.env`.

---

## `config.json` — Cấu Hình API

Chứa URL endpoint và query parameters. Đọc trực tiếp bởi `main.py`.

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

**Sửa file này khi:** Thêm địa điểm mới, thêm biến thời tiết/không khí, thay đổi endpoint.

---

## `config_runtime_constant.json` — Hằng Số Vận Hành

Chứa thông số kỹ thuật dễ thay đổi theo môi trường. Quản lý qua `ConfigManager`.

| Thông số | Giá trị | Ý nghĩa |
|---|---|---|
| `api.max_retries` | `3` | Số lần thử lại khi API fail |
| `api.backoff_factor` | `1` | Hệ số giãn cách: lần 2=2s, lần 3=4s |
| `api.timeout_sec` | `10` | Timeout request (giây) |
| `database.max_retries` | `3` | Số lần retry kết nối DB |
| `database.retry_delay_sec` | `5` | Chờ giữa các lần retry DB (giây) |

**Sửa file này khi:** Deploy lên Production (tăng timeout), API chập chờn (tăng retries).

---

## Tại sao tách 2 file?

| | `config.json` | `config_runtime_constant.json` |
|---|---|---|
| Bản chất | Cấu hình Business | Cấu hình Kỹ thuật |
| Ai sửa | Data Engineer | DevOps |
| Đọc bởi | `main.py` trực tiếp | `ConfigManager` Singleton |

# utils/ — Các Tiện Ích Dùng Chung (Shared Utilities)

## Nhiệm vụ
Cung cấp các công cụ nền tảng được tái dụng bởi mọi module trong `src/`: logging chuẩn hóa và quản lý cấu hình tập trung.

---

## File: `logger.py` — `get_logger(name)`

### Cách dùng
```python
from src.utils.logger import get_logger
logger = get_logger("TenModule")

logger.info("Thông tin thông thường")
logger.warning("Cảnh báo")
logger.error("Lỗi nghiêm trọng")
```

### Format log
```
2026-05-06 14:04:59 | PostgresLoader      | INFO     | Connection successful!
2026-05-06 14:04:59 | OpenMeteoExtractor  | ERROR    | HTTP Request failed...
```

### Chiến lược output theo môi trường

| Môi trường | Output | Lý do |
|---|---|---|
| **Docker/Airflow** (`AIRFLOW_HOME` được set) | Chỉ `stdout` | Airflow Worker tự bắt stdout và lưu vào Task Log. Ghi file trong container là vô nghĩa vì mất khi restart. |
| **Local** (máy dev, không có `AIRFLOW_HOME`) | `stdout` + file `logs/pipeline.log` | Tiện tra cứu khi debug. |

**Phát hiện môi trường Docker:**
```python
is_docker = os.getenv("AIRFLOW_HOME") or os.getenv("RUNNING_IN_DOCKER")
```
`AIRFLOW_HOME=/opt/airflow` được set sẵn bởi Airflow base image. Không cần thêm vào `.env`.

### Kỹ thuật tránh duplicate logs
```python
if not logger.handlers:
    # Chỉ thêm handler nếu logger chưa được cấu hình
    ...
```
Nếu nhiều module cùng gọi `get_logger("MainPipeline")`, Python trả về cùng 1 logger object (dùng `logging.getLogger(name)` là Singleton theo `name`). Guard `if not logger.handlers` đảm bảo không thêm handler trùng.

---

## File: `config_manager.py` — `class ConfigManager`

### Mục đích
Tách biệt các "Magic Numbers" (con số ma thuật như `timeout_sec=10`, `max_retries=5`) ra khỏi code logic. Theo triết lý **Twelve-Factor App**: cấu hình nên độc lập với code.

### Singleton Pattern
```python
class ConfigManager:
    _instance = None  # Class-level: chỉ tồn tại 1 bản duy nhất

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()  # Đọc file JSON đúng 1 lần
        return cls._instance
```

**Lợi ích Singleton:** Dù `ConfigManager()` được gọi 10 lần từ 10 module khác nhau, file JSON chỉ được đọc từ đĩa **đúng 1 lần** → tiết kiệm I/O.

### File config được nạp: `config_runtime_constant.json`
```json
{
  "api": {
    "max_retries": 3,
    "backoff_factor": 1,
    "timeout_sec": 30
  },
  "database": {
    "max_retries": 3,
    "retry_delay_sec": 5
  },
  "alert_thresholds": {
    "rain_probability_pct": 80,
    "rain_mm": 3.0,
    "pm25_alert_ugm3": 55.0,
    "uv_alert_index": 8.0,
    "heatwave_alert_temp": 38.0
  },
  "telegram_bot": {
    "districts": [
      {"label": "Quan 1", "db_name": "HCM Quận 1"}
    ]
  },
  "alert_job": {
    "scheduled_region_prefix": "",
    "sudden_region_prefix": "",
    "schedule_hours": {
      "morning": 6,
      "evening": 20
    }
  }
}
```

### Properties
| Property | Kiểu trả về | Dùng bởi |
|---|---|---|
| `api_config` | `dict` | `OpenMeteoExtractor` (lấy retry, timeout) |
| `database_config` | `dict` | `PostgresLoader` (lấy retry, delay) |
| `alert_thresholds` | `dict` | `bot_services.py` (UX/UI format), `alert_job.py` (query DB) |
| `telegram_bot_config` | `dict` | `telegram_bot.py` (build menu chọn Quận) |
| `alert_job_config` | `dict` | `alert_job.py` (lọc khu vực phát thanh cảnh báo) |

### Cơ chế Fallback (Phòng thủ & Zero Hardcode)
Nếu `config_runtime_constant.json` bị mất hoặc JSON lỗi cú pháp, `ConfigManager` **không crash**, nhưng nó tuyệt đối **KHÔNG** tự ý "chế" ra giá trị mặc định (Zero Hardcode). Nó sẽ trả về Dictionary rỗng `{}`:
```python
except Exception as e:
    logger.error(f"Failed to load config at {config_path}: {e}. Returning empty configurations.")
    self._config = {}
```
Việc xử lý Config được thiết kế theo mô hình **HYBRID (Lai tạo)** kết hợp giữa **Resilience (Kiên cường)** và **Fail-Fast (Chết sớm)**:

1. **Với tham số Kỹ thuật (Technical Params):** Dùng `.get("key", default)`. Ví dụ: `timeout`, `max_retries`. Nếu file cấu hình mất, hệ thống tự lui về fallback an toàn (VD: 3 lần thử lại, timeout 10s) để đảm bảo Core Pipeline vẫn hoạt động bình thường, không bao giờ để Data Pipeline chết vì mấy tham số lặt vặt.
2. **Với tham số Nghiệp vụ (Business/Structural Params):** Dùng `["key"]` (Fail-fast). Ví dụ: `districts` trong Bot, hoặc `target_region_prefix`. Nếu thiếu cấu hình này, UI không thể render, hoặc logic cốt lõi bị sai lệch. Hệ thống sẽ văng lỗi `KeyError` ngay lập tức để cảnh báo DevOps.

### Import sẵn instance global
```python
# Cuối file config_manager.py
config_manager = ConfigManager()  # Singleton được khởi tạo khi module được import lần đầu
```

**Cách dùng ở nơi khác:**
```python
from src.utils.config_manager import config_manager  # Import thẳng object, không phải class

# Cách 1 (Resilient): Cho tham số kỹ thuật
timeout = config_manager.api_config.get("timeout_sec", 10)

# Cách 2 (Fail-fast): Cho tham số nghiệp vụ trọng yếu
prefix = config_manager.alert_job_config["scheduled_region_prefix"]
```

---

## Tóm tắt quan hệ giữa 2 file utils

- `logger.py` → Công cụ quan sát (Observability)
- `config_manager.py` → Công cụ cấu hình (Configuration)

> Mọi module trong `src/` đều import cả 2. `ConfigManager` dùng `logger` để báo khi nạp config thành công/thất bại.

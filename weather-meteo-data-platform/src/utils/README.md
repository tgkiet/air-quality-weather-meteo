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
Tách biệt các "Magic Numbers" (con số ma thuật như `timeout=10`, `max_retries=3`) ra khỏi code logic. Theo triết lý **Twelve-Factor App**: cấu hình nên độc lập với code.

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
    "timeout_sec": 10
  },
  "database": {
    "max_retries": 3,
    "retry_delay_sec": 5
  }
}
```

### Properties
| Property | Kiểu trả về | Dùng bởi |
|---|---|---|
| `api_config` | `dict` | `OpenMeteoExtractor` (lấy retry, timeout) |
| `database_config` | `dict` | `PostgresLoader` (lấy retry, delay) |

### Cơ chế Fallback (Phòng thủ)
Nếu `config_runtime_constant.json` bị mất hoặc JSON lỗi cú pháp, `ConfigManager` **không crash**. Nó tự dùng giá trị mặc định an toàn:
```python
except FileNotFoundError:
    self._config = {
        "api": {"max_retries": 3, "backoff_factor": 1, "timeout_sec": 10},
        "database": {"max_retries": 3, "retry_delay_sec": 5}
    }
```

### Import sẵn instance global
```python
# Cuối file config_manager.py
config_manager = ConfigManager()  # Singleton được khởi tạo khi module được import lần đầu
```

**Cách dùng ở nơi khác:**
```python
from src.utils.config_manager import config_manager  # Import thẳng object, không phải class

timeout = config_manager.api_config.get("timeout_sec", 10)
```

---

## Tóm tắt quan hệ giữa 2 file utils

```
logger.py          → Công cụ quan sát (Observability)
config_manager.py  → Công cụ cấu hình (Configuration)

Mọi module trong src/ đều import cả 2.
ConfigManager dùng logger để báo khi nạp config thành công/thất bại.
```

# extractors/ — Lớp Thu Thập Dữ Liệu (Extract Layer)

## Nhiệm vụ
Chịu trách nhiệm **duy nhất**: giao tiếp với API bên ngoài và trả về dữ liệu thô dạng JSON. Không có bất kỳ logic nghiệp vụ, lưu trữ, hay biến đổi nào ở đây.

---

## File: `open_meteo.py` — `class OpenMeteoExtractor`

### Khởi tạo (`__init__(url: str)`)
Nhận URL của API endpoint. Toàn bộ cấu hình kỹ thuật (retry, timeout) được đọc tự động từ `ConfigManager`.

**Việc xảy ra khi khởi tạo:**
1. Tạo `requests.Session()` — mở một kênh TCP liên tục (Connection Pooling) để tái dụng cho nhiều request thay vì mở/đóng TCP mỗi lần. Giúp tăng tốc đáng kể khi gọi API nhiều lần.
2. Gắn `urllib3.util.retry.Retry` vào Session với chiến lược **Exponential Backoff**.

---

### Chiến lược Retry (Exponential Backoff)

```
Lần 1 thất bại → chờ 0 giây → thử lại
Lần 2 thất bại → chờ 2 giây → thử lại
Lần 3 thất bại → chờ 4 giây → raise lỗi
```

Chỉ retry với các mã HTTP báo lỗi SERVER hoặc QUÁ TẢI:
- `429` Too Many Requests
- `500` Internal Server Error
- `502` Bad Gateway
- `503` Service Unavailable
- `504` Gateway Timeout

**Không retry** với lỗi 4xx của client (ví dụ: `404 Not Found`, `400 Bad Request`) vì đó là lỗi do bạn chứ không phải server.

> Các thông số `max_retries` và `backoff_factor` được đọc từ `config_runtime_constant.json` qua `ConfigManager` — không hardcode trong code.

---

### Hàm chính: `get_open_meteo_data(params, expected_keys)`

**Tham số:**
| Tham số | Kiểu | Bắt buộc | Mô tả |
|---|---|---|---|
| `params` | `dict` | ✅ | Query parameters gửi lên API (latitude, longitude, hourly...) |
| `expected_keys` | `set` | ❌ | Tập hợp các key BẮT BUỘC phải có trong response JSON |

**Luồng xử lý bên trong:**
```
1. GET request → response.raise_for_status() (bắt 4xx/5xx)
2. response.json() → raw_data
3. isinstance check: raw_data có phải dict không?
4. Defensive check: raw_data có chứa {"error": true} không?
5. Data Contract: raw_data có đủ expected_keys không?
6. Nested check: nếu "hourly" trong expected_keys, raw_data["hourly"] có "time" không?
7. return raw_data ✅
```

**Exception handling:**
| Loại lỗi | Exception bắt | Raise ra |
|---|---|---|
| Sự cố mạng, HTTP error (4xx/5xx) | `requests.exceptions.RequestException` | `RuntimeError` với `from e` (giữ traceback gốc) |
| JSON không hợp lệ, vi phạm Data Contract | `ValueError` | `RuntimeError` với `from e` |

---

### Khái niệm cốt lõi: Data Contract (Hợp đồng Dữ liệu)

> **Vấn đề:** API bên ngoài có thể trả về HTTP 200 OK nhưng nội dung bên trong là thông báo bảo trì `{"message": "Service under maintenance"}`. Nếu không kiểm tra, dữ liệu rác này sẽ vào thẳng PostgreSQL.

> **Giải pháp (Dependency Injection):** `expected_keys` được caller (file `main.py`) định nghĩa và "tiêm" vào. `OpenMeteoExtractor` đóng vai trò kiểm soát biên giới nhưng KHÔNG tự quyết định hợp đồng là gì. Nhờ vậy, class này có thể tái dụng cho bất kỳ API nào của Open-Meteo mà không cần sửa code.

**Ví dụ gọi:**
```python
extractor = OpenMeteoExtractor("https://api.open-meteo.com/v1/forecast")

# Hợp đồng: response phải có latitude, longitude, hourly
data = extractor.get_open_meteo_data(
    params={"latitude": 10.77, "longitude": 106.70, "hourly": "temperature_2m", "timezone": "Asia/Bangkok"},
    expected_keys={"latitude", "longitude", "hourly"}
)
```

---

## Cách thêm Extractor mới

Nếu sau này cần lấy từ API khác (ví dụ NASA Air Quality), chỉ cần tạo file `nasa_aq.py` cùng thư mục và implement interface tương tự. `OpenMeteoExtractor` không cần sửa gì.

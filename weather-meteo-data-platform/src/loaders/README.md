# loaders/ — Lớp Tải Dữ Liệu (Load Layer)

## Nhiệm vụ
Chịu trách nhiệm **duy nhất**: tiếp nhận dữ liệu JSON thô từ Extractor và ghi an toàn vào PostgreSQL (Bronze Layer). Không có logic Extract hay Transform ở đây.

---

## File: `postgres_loader.py` — `class PostgresLoader`

### Khởi tạo (`__init__()`)

**Fail-fast Validation — Nguyên tắc "chết ngay từ cổng":**
Ngay tại `__init__`, class kiểm tra toàn bộ 5 biến môi trường bắt buộc. Nếu thiếu bất kỳ biến nào, raise `EnvironmentError` ngay lập tức thay vì để lỗi xuất hiện ở sâu bên trong khi đang chạy pipeline.

```python
required_vars = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT"]
```

> **Tại sao quan trọng?** Nếu DevOps quên cấu hình `.env`, pipeline sẽ báo lỗi rõ ràng `"Missing required environment variables: POSTGRES_PASSWORD"` thay vì văng lỗi `psycopg2.OperationalError` khó hiểu ở tận dưới.

---

### Hàm `connect()`

**Idempotent Connect — Chống rò rỉ connection:**
Kiểm tra `self.connection.closed` trước khi tạo connection mới. Nếu đã mở rồi, trả về connection cũ luôn. Tránh tình trạng gọi `connect()` 2 lần tạo ra 2 connection mà không đóng cái đầu.

**Retry với Exponential Backoff:**
Số lần thử và thời gian chờ lấy từ `config_runtime_constant.json` (không hardcode). Mỗi lần thất bại chỉ bắt `psycopg2.Error` (lỗi DB thuần túy), không bắt `Exception` chung để tránh nuốt lỗi lập trình.

```
Lần 1 thất bại → chờ 5 giây → thử lại
Lần 2 thất bại → chờ 5 giây → thử lại
Lần 3 thất bại → raise ConnectionError
```

---

### Hàm `insert_data(table_name, source_type, execution_date, raw_json)`

Đây là hàm cốt lõi nhất. Nó tích hợp đồng thời **3 kỹ thuật nâng cao**:

#### 1. Chống SQL Injection bằng `psycopg2.sql.Identifier`
```python
# ĐÚNG ✅ — psycopg2 tự escape table_name
sql.SQL("INSERT INTO {}").format(sql.Identifier(table_name))

# SAI ❌ — nguy cơ SQL Injection nếu table_name bị inject
f"INSERT INTO {table_name} ..."
```

#### 2. Context Manager `with cursor` — Chống rò rỉ con trỏ DB
```python
with self.connection.cursor() as cursor:
    cursor.execute(...)
# cursor tự động đóng khi thoát khỏi block 'with', dù có lỗi hay không
```

#### 3. UPSERT thay vì DELETE + INSERT — Tối ưu MVCC PostgreSQL

**Vấn đề với DELETE + INSERT:**
PostgreSQL dùng cơ chế MVCC (Multi-Version Concurrency Control). Khi `DELETE`, bản ghi không bị xóa ngay khỏi ổ cứng mà chỉ bị đánh dấu là "Dead Tuple". Sau đó `INSERT` lại tạo bản ghi mới. Chạy 1 lần/giờ → bảng chứa đầy rác Dead Tuples → hệ thống phải chạy Auto-Vacuum tốn CPU.

**Giải pháp UPSERT:**
```sql
INSERT INTO api_openmeteo_raw_data (source_type, execution_date, raw_json)
VALUES (%s, %s, %s)
ON CONFLICT (source_type, execution_date)
DO UPDATE SET raw_json = EXCLUDED.raw_json
```
Chỉ 1 thao tác I/O vật lý. Nếu chưa có → INSERT. Nếu đã có → UPDATE tại chỗ. **Không sinh Dead Tuple.**

> **Điều kiện tiên quyết:** Bảng phải có `UNIQUE CONSTRAINT (source_type, execution_date)`. Đã được tạo trong `scripts/init_dbs.sh`.

#### 4. Tính Lũy Đẳng (Idempotency) — Pipeline an toàn khi Retry

`execution_date` là "khóa tự nhiên" (Natural Key) của mỗi batch dữ liệu. Nó được truyền vào từ `main.py`, lấy từ Airflow Jinja Template `{{ logical_date | ts }}`.

**Kịch bản minh họa:**
- Airflow lập lịch chạy batch `10:00:00`
- Task thực hiện lúc 10h00 → Insert thành công → Commit
- **Nếu** task crash, Airflow retry lúc 10h05
- `execution_date` vẫn là `10:00:00` (Logical Date, không phải `datetime.now()`)
- UPSERT sẽ tìm thấy dòng `(source_type, "10:00:00")` đã có → UPDATE thay vì INSERT mới
- **Kết quả: DB luôn chỉ có 1 dòng duy nhất cho batch này, dù chạy bao nhiêu lần**

#### 5. `psycopg2.extras.Json` thay vì `json.dumps()`
```python
# ĐÚNG ✅ — C-driver của psycopg2, xử lý JSONB natively
cursor.execute(query, (source_type, execution_date, extras.Json(raw_json)))

# KÉM ❌ — Python serialize → string → Postgres parse lại
cursor.execute(query, (source_type, execution_date, json.dumps(raw_json)))
```

---

### Hàm `close()`
Kiểm tra `connection.closed` trước khi đóng để tránh lỗi khi `close()` được gọi nhiều lần (idempotent close).

---

### Exception Handling

| Exception | Hành động |
|---|---|
| `psycopg2.Error` | Log lỗi DB cụ thể → Rollback → raise |
| `Exception` (bất ngờ) | Log lỗi unexpected → Rollback → raise |

Luôn `raise` sau khi Rollback để Airflow nhận biết task **FAILED** (tránh "Silent Failure" — lỗi ngầm Airflow vẫn báo thành công).

---

### Sơ đồ luồng hoàn chỉnh

```
PostgresLoader()
  ├── __init__()     → Validate 5 biến môi trường (Fail-fast)
  ├── connect()      → Retry 3 lần → Lưu connection
  ├── insert_data()  → Check connection sống → UPSERT với Idempotency
  └── close()        → Đóng connection an toàn
```

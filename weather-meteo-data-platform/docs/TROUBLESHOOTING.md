# Xử Lý Sự Cố (Troubleshooting)

Hướng dẫn chẩn đoán và khắc phục các lỗi thường gặp trong hệ thống.

---

## Chẩn Đoán Nhanh

```bash
# Xem trạng thái các containers
docker ps

# Xem log Airflow (real-time)
docker logs -f airflow_container

# Xem log Postgres
docker logs -f postgres_container

# Xem log pipeline Python
cat logs/pipeline.log | tail -n 100
```

---

## Lỗi Thường Gặp

### Không đăng nhập được Airflow UI — "Invalid credentials"

**Nguyên nhân có thể:**
- Biến `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` trong `.env` chưa đúng
- Container khởi động chưa xong (lệnh `airflow users create` chưa chạy xong)

**Cách fix:**
```bash
# Kiểm tra user đã được tạo chưa
docker exec airflow_container airflow users list

# Reset password trực tiếp (không cần restart)
docker exec airflow_container airflow users reset-password -u <username> -p <new_password>
```

---

### Airflow container crash ngay khi khởi động

**Kiểm tra log:**
```bash
docker logs airflow_container | tail -n 50
```

**Nguyên nhân phổ biến và cách fix:**

| Thông báo lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `connection refused` | Postgres chưa sẵn sàng | `docker compose restart airflow` |
| `Database migration required` | `db migrate` và services khởi động cùng lúc | Chạy lại `docker compose down -v && docker compose up -d` |
| `AIRFLOW_DB_USER` not found | Biến `.env` sai tên | Kiểm tra lại tên biến trong `.env` |

---

### Task `fetch_data` bị FAILED trong Airflow

**Bước 1 — Xem log trong Airflow UI:**
```
DAGs → open_meteo_api_pipeline_orchestrator → fetch_data → Logs
```

**Bước 2 — Xem log Python pipeline:**
```bash
cat logs/pipeline.log | grep -i "error" | tail -n 20
```

**Nguyên nhân phổ biến:**

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `connection refused` (port 5432) | Postgres chưa khởi động hoặc sai `POSTGRES_HOST` | Kiểm tra `docker-compose.yml` có inject `POSTGRES_HOST=postgres_db` chưa |
| `password authentication failed` | Sai `POSTGRES_USER` hoặc `POSTGRES_PASSWORD` | Kiểm tra `.env` và chạy `docker compose down -v && docker compose up -d` |
| `relation does not exist` | Bảng `api_openmeteo_raw_data` chưa được tạo | `init_dbs.sh` chưa chạy — xem mục bên dưới |
| `Failed to fetch data after 3 attempts` | Lỗi mạng hoặc API Open-Meteo down | Kiểm tra kết nối internet, thử lại sau |
| `Missing configuration file` | `src/config/config.json` không tìm thấy | Kiểm tra volume mount trong `docker-compose.yml` |

---

### Postgres không tạo được 2 Database

**Dấu hiệu:** Task pipeline lỗi `relation does not exist` hoặc `database does not exist`.

**Nguyên nhân:** `init_dbs.sh` chỉ chạy **một lần duy nhất** khi volume PostgreSQL chưa tồn tại. Nếu volume cũ từ lần trước còn đó (chạy `docker compose down` mà không có `-v`), Postgres sẽ bỏ qua script init.

**Cách fix dứt điểm:**
```bash
# ⚠️ Cảnh báo: Lệnh này xóa TOÀN BỘ dữ liệu PostgreSQL
docker compose down -v
docker compose up -d
```

**Kiểm tra Database đã được tạo chưa:**
```bash
docker exec postgres_container psql -U <POSTGRES_USER> -l
```

---

### DAG không xuất hiện trong Airflow UI

**Kiểm tra:**
```bash
# Xem DAG processor có phát hiện file không
docker logs airflow_container | grep "orchestrator.py"

# Xem lỗi parse DAG
docker logs airflow_container | grep -i "error\|import error"
```

**Nguyên nhân phổ biến:**

| Lỗi | Cách fix |
|---|---|
| Volume `./airflow/dags` không mount đúng | Kiểm tra `docker-compose.yml` |
| Lỗi syntax trong `orchestrator.py` | Chạy `python -c "import ast; ast.parse(open('airflow/dags/orchestrator.py').read())"` |
| DAG bị paused | Vào UI bật toggle Unpause |

---

### Pipeline kết nối DB thành công ở local nhưng FAILED trong Airflow

**Nguyên nhân:** Khi chạy bên trong Docker, phải dùng tên service `postgres_db` làm host, không phải `localhost`.

**Kiểm tra `docker-compose.yml`:**
```yaml
services:
  airflow:
    environment:
      POSTGRES_HOST: postgres_db   # Đúng — tên service trong Docker network
      POSTGRES_PORT: 5432          # Port nội bộ trong Docker
```

Đảm bảo 2 dòng này có trong section `environment` của service `airflow`.

---

## Lệnh Hữu Ích

```bash
# Xem danh sách DAGs
docker exec airflow_container airflow dags list

# Trigger DAG thủ công
docker exec airflow_container airflow dags trigger open_meteo_api_pipeline_orchestrator

# Xem lịch sử chạy DAG
docker exec airflow_container airflow dags list-runs -d open_meteo_api_pipeline_orchestrator

# Kiểm tra kết nối Postgres từ trong container Airflow
docker exec airflow_container python3 -c "
import psycopg2, os
conn = psycopg2.connect(
    host='postgres_db', port=5432,
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)
print('Kết nối thành công!')
conn.close()
"

# Xem số record trong bảng Bronze
docker exec postgres_container psql -U <POSTGRES_USER> -d air_quality_db \
    -c "SELECT source_type, COUNT(*) FROM api_openmeteo_raw_data GROUP BY source_type;"
```

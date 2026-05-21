# Setup & Cấu Hình

Hướng dẫn đầy đủ để thiết lập môi trường, cấu hình biến và chạy hệ thống từ đầu.

---

## Yêu Cầu Hệ Thống

| Công Cụ | Phiên Bản Tối Thiểu | Kiểm Tra |
|---|---|---|
| Docker Engine | ≥ 24.x | `docker --version` |
| Docker Compose | ≥ 2.x | `docker compose version` |
| RAM | ≥ 4 GB | Airflow cần ít nhất 2 GB |
| Disk | ≥ 5 GB | Cho PostgreSQL volumes + Docker images |

---

## Tạo File `.env`

> **File `.env` KHÔNG được commit lên Git.** Tạo thủ công tại thư mục `weather-meteo-data-platform/`.

```env
# --- CẤU HÌNH DATABASE CHÍNH (RAW DATA) ---
POSTGRES_USER=your_db_user
POSTGRES_PASSWORD=your_strong_password
POSTGRES_DB=air_quality_db
POSTGRES_HOST=localhost      # Khi chạy local (ngoài Docker)
POSTGRES_PORT=5434           # Port mapping ra máy host

# --- CẤU HÌNH DATABASE CHO AIRFLOW (METADATA) ---
AIRFLOW_DB_NAME=airflow_db
AIRFLOW_DB_USER=airflow_db_user
AIRFLOW_DB_PASSWORD=your_airflow_db_password

# --- CẤU HÌNH TÀI KHOẢN ĐĂNG NHẬP AIRFLOW UI ---
_AIRFLOW_WWW_USER_USERNAME=your_username
_AIRFLOW_WWW_USER_PASSWORD=your_ui_password

# --- CẤU HÌNH AIRFLOW HỆ THỐNG ---
AIRFLOW_UID=1000
AIRFLOW_API_SECRET_KEY=your_random_64_byte_hex_key
```

---

## Giải Thích Biến Môi Trường

| Biến | Mô Tả | Dùng ở đâu |
|---|---|---|
| `POSTGRES_USER` | User của DB chứa raw data | `PostgresLoader`, `init_dbs.sh` |
| `POSTGRES_PASSWORD` | Password của DB chứa raw data | `PostgresLoader`, `init_dbs.sh` |
| `POSTGRES_DB` | Tên DB chứa raw data | `PostgresLoader`, `init_dbs.sh` |
| `POSTGRES_HOST` | Host kết nối DB | `PostgresLoader` |
| `POSTGRES_PORT` | Port mapping ra ngoài host | `PostgresLoader` |
| `AIRFLOW_DB_NAME` | Tên DB metadata của Airflow | `docker-compose.yml`, `init_dbs.sh` |
| `AIRFLOW_DB_USER` | User của DB Airflow | `docker-compose.yml`, `init_dbs.sh` |
| `AIRFLOW_DB_PASSWORD` | Password của DB Airflow | `docker-compose.yml`, `init_dbs.sh` |
| `_AIRFLOW_WWW_USER_USERNAME` | Tên đăng nhập Airflow Web UI | `docker-compose.yml` command |
| `_AIRFLOW_WWW_USER_PASSWORD` | Mật khẩu Airflow Web UI | `docker-compose.yml` command |
| `AIRFLOW_UID` | UID user chạy Airflow trong container | `docker-compose.yml` |
| `AIRFLOW_API_SECRET_KEY` | Secret key bảo vệ API endpoint Airflow | `docker-compose.yml` |

> **Quan trọng về `POSTGRES_HOST`:**
> - Khi chạy `python src/main.py` **trực tiếp trên máy**: `POSTGRES_HOST=localhost`, `POSTGRES_PORT=5434`
> - Khi Airflow chạy task **bên trong Docker container**: `POSTGRES_HOST=postgres_db`, `POSTGRES_PORT=5432`
>
> Docker Compose tự động inject `POSTGRES_HOST=postgres_db` vào Airflow container, ghi đè giá trị `localhost` trong `.env`. Đây là cơ chế **Separation of Concerns** — code không cần biết mình đang chạy ở đâu.

---

## Cấu Hình API (`src/config/config.json`)

File `src/config/config.json` là **nguồn sự thật duy nhất** cho tất cả cấu hình API. Không hardcode bất kỳ giá trị nào trong file Python.

```json
{
  "api": {
    "open_meteo": {
      "weather_url": "https://api.open-meteo.com/v1/forecast",
      "weather_params": {
        "latitude": 10.7756,
        "longitude": 106.7019,
        "hourly": "temperature_2m,relative_humidity_2m,...",
        "timezone": "Asia/Bangkok"
      },
      "aq_url": "https://air-quality-api.open-meteo.com/v1/air-quality",
      "aq_params": {
        "latitude": 10.7756,
        "longitude": 106.7019,
        "hourly": "pm10,pm2_5,carbon_monoxide,...",
        "timezone": "Asia/Bangkok"
      }
    }
  }
}
```

**Thay đổi vị trí địa lý:** Sửa `latitude` và `longitude`.  
**Thêm biến dữ liệu:** Thêm tên biến vào chuỗi `hourly`. Tham khảo [Open-Meteo Docs](https://open-meteo.com/en/docs).

---

## Khởi Chạy Hệ Thống

### Lần đầu (First-time setup)

```bash
cd weather-meteo-data-platform/
docker compose up -d --build
```

Quá trình tự động diễn ra theo thứ tự:
1. **Build** Custom Airflow Image (cài `psycopg2`, `requests`, `python-dotenv`)
2. **Khởi động** PostgreSQL → tự chạy `init_dbs.sh` → tạo `air_quality_db` và `airflow_db`
3. **Khởi động** Airflow → migrate DB → tạo tài khoản Web UI
4. **Bật** 4 tiến trình: `scheduler`, `triggerer`, `dag-processor`, `api-server`

### Reset toàn bộ (xóa sạch data)

> Lệnh `-v` xóa **toàn bộ dữ liệu** PostgreSQL. Chỉ dùng khi thay đổi cấu hình DB cốt lõi.

```bash
docker compose down -v && docker compose up -d
```

### Restart bình thường (giữ nguyên data)

```bash
docker compose down && docker compose up -d
```

### Kiểm tra trạng thái

```bash
docker ps                          # Xem containers đang chạy
docker logs -f airflow_container   # Xem log real-time Airflow
docker logs -f postgres_container  # Xem log Postgres
```

---

## Đăng Nhập Airflow Web UI

- **URL:** `http://localhost:8080`
- **Username:** Giá trị `_AIRFLOW_WWW_USER_USERNAME` trong `.env`
- **Password:** Giá trị `_AIRFLOW_WWW_USER_PASSWORD` trong `.env`

**Reset password (không cần tắt hệ thống):**
```bash
docker exec airflow_container airflow users reset-password -u <username> -p <new_password>
```

---

## Chạy Pipeline Thủ Công (Không Dùng Airflow)

Dùng để debug hoặc test nhanh:

```bash
# Đảm bảo POSTGRES_HOST=localhost trong .env trước khi chạy
python src/main.py
```

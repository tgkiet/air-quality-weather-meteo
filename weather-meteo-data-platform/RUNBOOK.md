# Data Platform Runbook

Hướng dẫn vận hành hệ thống Vietnam Weather & Air Quality Data Platform từ con số 0.

---

## 1. KHỞI TẠO (Prerequisites & Startup)
Đảm bảo bạn đang đứng ở thư mục gốc của dự án: `air-quality-weather-meteo/weather-meteo-data-platform/`
**1.1. Cấu hình & Chạy hệ thống**
```bash
cp .env.example .env
# Sinh key ngẫu nhiên cho Superset và điền vào .env (SUPERSET_SECRET_KEY)
openssl rand -base64 42

# Khởi động cụm 7 containers (DB, Redis, Airflow, Superset)
docker compose up -d --build
```
> *Đợi 2-3 phút. Kiểm tra trạng thái: `docker compose ps`*

---

## 2. NẠP DỮ LIỆU LỊCH SỬ (Data Backfill)

**2.1. Làm sạch dữ liệu cũ (Tùy chọn)**
Để đập đi xây lại data **KHÔNG mất dữ liệu Dashboard Superset**, chạy lệnh làm trống các bảng gốc:
```bash
source .env && docker exec -it postgres_container psql -U "$POSTGRES_USER" -d air_quality_db -c "TRUNCATE TABLE bronze_historical_weather, api_openmeteo_raw_data, silver_layer.alert_history CASCADE;"
```

**2.2. Kéo dữ liệu từ Archive API (2022 - nay)**
Kéo data tự động cho toàn bộ 52 khu vực (30 HN + 22 HCM). Quá trình mất ~15 phút.
```bash
docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py --location-prefix HN --start-date 2022-08-02 --end-date 2026-05-29
```

```bash
docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py --location-prefix HCM --start-date 2022-08-02 --end-date 2026-05-29
```

---

## 3. BIẾN ĐỔI & KIỂM ĐỊNH (dbt Transform & Test)

Dùng dbt để làm sạch (Silver) và gộp bảng (Gold). Chạy 1 lệnh gộp duy nhất để Build & Test (29 Data Quality Gates):
```bash
docker exec airflow_container bash -c "dbt run --full-refresh --project-dir /opt/airflow/dbt-transform --profiles-dir /home/airflow/.dbt && dbt test --project-dir /opt/airflow/dbt-transform --profiles-dir /home/airflow/.dbt"
```
> *Kỳ vọng: `PASS=29 WARN=0 ERROR=0`*

---

## 4. GIAO DIỆN QUẢN TRỊ (Airflow & Superset)

**4.1. Airflow (http://localhost:8080)**
- **Đăng nhập:** Bằng `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` trong `.env`.
- **Tác vụ:** Unpause DAG `open_meteo_api_pipeline_orchestrator` để chạy Realtime mỗi giờ.

**4.2. Superset (http://localhost:8088)**
- **Đăng nhập:** `admin` / `SUPERSET_ADMIN_PASSWORD` trong `.env`.
- **Kết nối Database:** 
  1. Settings → Database Connections → + DATABASE → PostgreSQL.
  2. URI: `postgresql+psycopg2://superset_user:<SUPERSET_DB_PASSWORD>@postgres_db:5432/air_quality_db`
  3. **Advanced → Other → ENGINE PARAMETERS (Sửa lỗi lệch múi giờ 7 tiếng):**
     `{"connect_args": {"options": "-c timezone=Asia/Bangkok"}}`
  4. Test Connection → Connect.

---

## 5. LỆNH BẢO TRÌ NÂNG CAO

```bash
# Xem logs
docker compose logs -f airflow
docker compose logs -f superset

# Dừng hệ thống (Giữ nguyên Data)
docker compose stop

# ⚠️ XÓA SẠCH MỌI THỨ (Bao gồm cả Superset Dashboard) ⚠️
docker compose down -v && docker compose up -d --build
```

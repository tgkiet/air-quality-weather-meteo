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
docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py --location-prefix HN --start-date 2022-08-02 --end-date 2026-05-30
```

```bash
docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py --location-prefix HCM --start-date 2022-08-02 --end-date 2026-05-30
```

---

## 3. BIẾN ĐỔI & KIỂM ĐỊNH (dbt Transform & Test)

Dùng dbt để làm sạch (Silver) và gộp bảng (Gold). Chạy 1 lệnh duy nhất để Build & Test (32 Data Quality Gates):
```bash
docker exec airflow_container bash -c "dbt run --full-refresh --project-dir /opt/airflow/dbt-transform --profiles-dir /home/airflow/.dbt && dbt test --project-dir /opt/airflow/dbt-transform --profiles-dir /home/airflow/.dbt"
```
> *Kỳ vọng: `PASS=32 WARN=0 ERROR=0`*

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
  3. **Advanced → Other → ENGINE PARAMETERS:**
     `{"connect_args": {"options": "-c timezone=Asia/Bangkok"}}`
     *(Lưu ý: Cấu hình này sửa lỗi lệch múi giờ khi Gom nhóm/Aggregation. Tuy nhiên, khi vẽ biểu đồ Time-series (ECharts), bạn **BẮT BUỘC** phải chọn cột `forecast_time_local` làm Time Column / Trục X để không bị lỗi lệch 7 tiếng).*
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
> [!CAUTION]
> Cờ `-v` sẽ xóa vĩnh viễn Named Volume `postgres_data`, gây mất toàn bộ dữ liệu Data Warehouse thu thập được.
> Hãy Backup trước khi chạy: `docker exec postgres_container pg_dump -U gkinhere air_quality_db > backup.sql`
docker compose down -v && docker compose up -d --build
```

---

## 6. DISASTER RECOVERY

> [!IMPORTANT]
> Sau khi khôi phục database từ backup hoặc rebuild lại volume, **PHẢI** chạy `--full-refresh` để đưa toàn bộ historical data vào Silver layer. Airflow DAG chạy incremental theo giờ **sẽ không tự động restore** historical data.

**6.1. Trình tự khôi phục sau mất dữ liệu hoàn toàn:**
```bash
# Bước 1: Backfill historical Bronze (chờ API reset 07:00 ICT nếu cần)
docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py \
  --location-prefix HN --start-date 2022-08-02 --end-date $(date +%Y-%m-%d)
docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py \
  --location-prefix HCM --start-date 2022-08-02 --end-date $(date +%Y-%m-%d)

# Bước 2: Rebuild toàn bộ Silver + Gold từ scratch
docker exec airflow_container bash -c \
  "dbt run --full-refresh --project-dir /opt/airflow/dbt-transform --profiles-dir /home/airflow/.dbt"

# Bước 3: Validate
docker exec airflow_container bash -c \
  "dbt test --project-dir /opt/airflow/dbt-transform --profiles-dir /home/airflow/.dbt"
```

> [!NOTE]
> Script backfill có **Smart Skip**: Nếu một quận/huyện đã có đủ data trong Bronze **cho đúng khoảng thời gian được yêu cầu** (≥ 95% số giờ lý thuyết trong `[start_date, end_date]`), nó sẽ được bỏ qua tự động mà không tốn API quota. Logic này **nhận biết đúng date range** — nếu bạn tắt hệ thống 1 tháng rồi backfill lại đúng khoảng đó, script sẽ phát hiện thiếu data và fetch bình thường. Lệnh trên an toàn để chạy lại nhiều lần (idempotent).

**6.2. Open-Meteo API Limit:**
- **Hourly limit:** Reset sau mỗi 60 phút.
- **Daily limit:** Reset lúc 00:00 UTC (07:00 ICT). Nếu bị 429 Daily Limit, script sẽ tự dừng ngay (`SystemExit(2)`).
- Thứ tự an toàn: Chạy HN trước, HCM sau. Mỗi prefix ~30-45 API requests.

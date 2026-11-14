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
> Kiểm tra trạng thái bằng `docker compose ps`. DAG mới được tạo ở trạng thái pause. Bật DAG sau khi đã điền cấu hình API/bot và kiểm tra stack.

---

## 2. NẠP DỮ LIỆU LỊCH SỬ (Data Backfill)

**2.1. Làm sạch dữ liệu trên môi trường thử nghiệm (Tùy chọn)**
Để đập đi xây lại data **KHÔNG mất dữ liệu Dashboard Superset**, chạy lệnh làm trống các bảng gốc:
```bash
source .env && docker exec -it postgres_container psql -U "$POSTGRES_USER" -d air_quality_db -c "TRUNCATE TABLE bronze_historical_weather, api_openmeteo_raw_data, silver_layer.alert_history CASCADE;"
```

**2.2. Kéo dữ liệu từ Archive API (2022 - nay)**
Kéo data tự động cho toàn bộ 52 khu vực (30 HN + 22 HCM). Quá trình mất ~15 phút.
```bash
docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py --location-prefix HN --start-date 2022-08-02 --end-date 2026-06-30
```

```bash
docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py --location-prefix HCM --start-date 2022-08-02 --end-date 2026-06-30
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
> Script backfill có **Smart Skip**: Nếu một quận/huyện đã có đủ data trong Bronze **cho đúng khoảng thời gian được yêu cầu** (≥ 95% số giờ lý thuyết trong `[start_date, end_date]`), nó sẽ được bỏ qua tự động mà không tốn API quota.
> **Mẹo vá lỗi (Gap Filling):** Đừng backfill từ tận năm 2022 nếu bạn chỉ tắt máy vài ngày! Hãy thu hẹp `--start-date` và `--end-date` sát với những ngày bị thiếu. Nếu bạn để range quá rộng (vd 3 năm), lượng data khổng lồ đã có sẵn sẽ lấn át vài ngày bị thiếu (kéo tỷ lệ lên >95%), khiến Smart Skip bỏ qua toàn bộ và không vá lỗi cho bạn. Lệnh trên an toàn để chạy lại nhiều lần (idempotent).

**6.2. Open-Meteo API Limit:**
- **Hourly limit:** Reset sau mỗi 60 phút.
- **Daily limit:** Reset lúc 00:00 UTC (07:00 ICT). Nếu bị 429 Daily Limit, script sẽ tự dừng ngay (`SystemExit(2)`).
- Thứ tự an toàn: Chạy HN trước, HCM sau. Mỗi prefix ~30-45 API requests.

## Báo cáo chất lượng dữ liệu

Các báo cáo chạy ở chế độ chỉ đọc và xuất CSV. Chạy từ thư mục platform:

```bash
docker compose exec -T airflow python -m src.scripts.data_report coverage \
  --city HN --start 2026-07-01 --end 2026-11-13

docker compose exec -T airflow python -m src.scripts.data_report temperature \
  --city HN --start 2026-11-01 --end 2026-11-14

docker compose exec -T airflow python -m src.scripts.data_report rain \
  --city HN --start 2026-11-01 --end 2026-11-14
```

`coverage` xuất cả ngày thiếu hoàn toàn và số giờ có nhiệt độ, PM2.5 trên tổng số giờ kỳ vọng của các điểm cấu hình. `temperature` tính MAE và sai lệch trung bình theo nhóm tầm dự báo 0–24, 24–48 và 48–72 giờ, dùng thời điểm lấy từ Bronze. Mốc thời gian đó là thời điểm hệ thống lấy bản dự báo, không phải giờ khởi tạo mô hình. `rain` đếm cảnh báo đúng, hụt và nhầm theo ngưỡng lượng mưa và xác suất trong cấu hình. Nguồn tham chiếu hiện tại là dữ liệu lịch sử Open-Meteo, không phải số đo trạm. Kết quả chỉ có tiêu đề CSV nghĩa là chưa có đủ cặp dự báo–tham chiếu trong thời gian đã chọn.

## Nâng cấp một stack đã có dữ liệu

1. Build image và chạy test cô lập theo mục bên dưới trước khi đặt lịch bảo trì.
2. Từ Airflow UI, pause DAG và đợi lượt đang chạy hoàn tất. Dừng bot, Airflow và các tiến trình Superset đang ghi metadata. Nếu stack đang tắt, chỉ khởi động lại container PostgreSQL hiện có để sao lưu.
3. Tạo bản sao lưu PostgreSQL chứa warehouse, metadata Airflow và Superset bên ngoài repo. File dump chứa dữ liệu vận hành, role và cấu hình kết nối. Lưu riêng khóa Superset dùng để giải mã các kết nối đã lưu.
4. Áp dụng migration SQL vào DB hiện tại, rồi chạy lại dbt để điền nguồn và thời điểm ingest cho dữ liệu Silver/Gold.
5. Kiểm tra Airflow, Superset và bot. Bật lại DAG sau khi kiểm tra.

```bash
docker compose stop airflow telegram_bot superset superset-worker superset-worker-beat
# Dùng container hiện có để sao lưu trước khi pull/recreate PostgreSQL.
docker compose start postgres_db
backup_dir="$HOME/meteo-backups"
mkdir -p -m 700 "$backup_dir"
backup_file=$(mktemp "$backup_dir/postgres-before-upgrade.XXXXXX")
docker compose exec -T postgres_db sh -c 'pg_dumpall -U "$POSTGRES_USER"' > "$backup_file" || exit 1
test -s "$backup_file" || exit 1
printf 'Backup: %s\n' "$backup_file"

# Mỗi migration có thể chạy lại trên DB đã áp dụng.
for migration in src/scripts/migrations/*.sql; do
  docker compose exec -T postgres_db sh -c \
    'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < "$migration" || exit 1
done
```

Xác nhận toàn bộ migration thành công trước khi tiếp tục. Khởi động PostgreSQL và Airflow đã nâng cấp, rồi dựng lại Gold và kiểm tra:

```bash
docker compose pull postgres_db
docker compose up -d --build --wait postgres_db airflow
docker compose exec -T airflow dbt build --full-refresh \
  --project-dir /opt/airflow/dbt-transform --profiles-dir /home/airflow/.dbt
docker compose up -d --build --wait
```

Bản full refresh có thể tốn thêm dung lượng tạm. Nếu phải quay lại phiên bản Airflow cũ sau khi chạy migration metadata, dùng bản sao lưu trên một PostgreSQL riêng rồi trỏ image cũ vào bản đã phục hồi. Giữ volume hiện tại làm dữ liệu đối chiếu. Dữ liệu dự báo và dữ liệu nhập lịch sử có cột nguồn riêng. Nếu một giờ có hai nguồn, Silver ưu tiên dự báo. Trong cùng nguồn, batch mới hơn được ưu tiên.


## Telegram

Các mốc 06:00 và 20:00 là giờ logic theo múi giờ Việt Nam. Tin nhắn được gửi sau khi các bước thu thập, dbt và kiểm tra dữ liệu hoàn thành. Bản tin cá nhân ở mốc sáng chỉ gửi cho chat riêng đã lưu điểm và bật đăng ký. Cấu hình tài khoản bot và chat broadcast bằng `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`.

Nếu Telegram trả lỗi xác nhận từ chối, tác vụ giải phóng lượt giữ chỗ để Airflow retry. Nếu bị timeout mà chưa biết Telegram đã nhận hay chưa, record vẫn còn trong `silver_layer.telegram_delivery` với `delivered_at IS NULL`. Đối chiếu lịch sử chat trước khi xử lý thủ công. Retry tự động giữ lại trạng thái này để tránh gửi trùng.

## Kiểm tra trên database cô lập

```bash
docker build -t meteo-checks:local .
bash tests/verify_airflow.sh meteo-checks:local postgres:16.16-alpine
bash tests/verify_warehouse.sh meteo-checks:local postgres:16.16-alpine
```

Các runner dùng network nội bộ và dữ liệu mẫu. CI cũng chạy unit test cho khởi động Airflow, ingest và Telegram. Không cần token hay gọi API bên ngoài.

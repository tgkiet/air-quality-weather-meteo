# Data Platform Runbook (Hướng Dẫn Vận Hành)

> **Runbook** này cung cấp hướng dẫn từng bước (Step-by-step) để khởi chạy, nạp dữ liệu và kiểm thử toàn bộ hệ thống Vietnam Weather & Air Quality Data Platform từ con số 0.

---

## TRƯỚC KHI BẮT ĐẦU (Prerequisites)

Đảm bảo bạn đang đứng ở thư mục gốc của dự án: `air-quality-weather-meteo/weather-meteo-data-platform/`

**Bước 0.1: Cấu hình biến môi trường**
Hệ thống sử dụng file `.env` làm trung tâm (Single Source of Truth) để bảo mật thông tin.
```bash
cp .env.example .env
```
Mở file `.env` vừa tạo và điền đầy đủ các giá trị. Đặc biệt lưu ý biến `SUPERSET_SECRET_KEY` là bắt buộc. Bạn có thể sinh một chuỗi ngẫu nhiên bằng lệnh:
```bash
openssl rand -base64 42
```

---

## GIAI ĐOẠN 1: KHỞI ĐỘNG HẠ TẦNG (Infrastructure Startup)

**Mục đích:** Dựng cụm 7 containers (PostgreSQL, Redis, Airflow và 4 nodes của Superset) đồng thời tự động khởi tạo các cơ sở dữ liệu và cấu hình phân quyền (RBAC).

**Bước 1.1: Chạy hệ thống**
```bash
docker compose up -d --build
```
> *Lưu ý: Quá trình này mất khoảng 2-3 phút để tải image và cài đặt thư viện. Cơ chế Healthcheck sẽ đảm bảo các services khởi động đúng thứ tự phụ thuộc (ví dụ: DB phải chạy xong thì Airflow mới được phép kết nối).*

**Bước 1.2: Kiểm tra trạng thái**
Đảm bảo tất cả 7 containers đều đang ở trạng thái `Up (healthy)`.
```bash
docker compose ps
```
Để theo dõi quá trình cài đặt thư viện và khởi chạy của Superset, hãy xem log:
```bash
docker compose logs superset --tail=50
```

---

## GIAI ĐOẠN 2: NẠP DỮ LIỆU LỊCH SỬ (Data Backfill)

Giai đoạn này nạp toàn bộ dữ liệu lịch sử từ 2022 đến nay vào Tầng Bronze. Dữ liệu bao gồm các trạm quan trắc tại Hà Nội và TP.HCM. Cơ chế `UPSERT` sẽ đảm bảo không có dữ liệu nào bị trùng lặp (Idempotency).

**Bước 2.1: Nạp file CSV Hà Nội (2022 - 11/2025)**
Nạp ~900k dòng lịch sử cũ cực nhanh bằng phương pháp `COPY EXPERT`.
```bash
docker exec airflow_container python3 /opt/airflow/src/scripts/load_historical_csvs.py
```

**Bước 2.2: Kéo dữ liệu API lịch sử TP.HCM (2022 - Hiện tại)**
Mất khoảng 10-15 phút do phải lấy qua Archive API.
```bash
docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py \
  --location-prefix HCM --start-date 2022-08-02 --end-date 2026-05-24
```

**Bước 2.3: Kéo bù khoảng trống (Gap-fill) cho Hà Nội**
Kéo phần dữ liệu từ sau khi CSV kết thúc cho đến thời điểm hiện tại. Mất khoảng 2-3 phút.
```bash
docker exec airflow_container python3 /opt/airflow/src/scripts/backfill_history.py \
  --location-prefix HN --start-date 2025-11-30 --end-date 2026-05-24
```

---

## GIAI ĐOẠN 3: BIẾN ĐỔI VÀ KIỂM ĐỊNH CHẤT LƯỢNG (dbt Transform & Test)

**Mục đích:** Sử dụng `dbt` để làm sạch dữ liệu từ Tầng Bronze, chuyển lên Silver và gộp lại ở dạng phẳng (Denormalized) tại Tầng Gold để BI Tool sử dụng.

**Bước 3.1: Build toàn bộ mô hình dữ liệu**
Chạy lệnh sau với cờ `--full-refresh` để xây dựng lại cấu trúc bảng từ đầu.
```bash
docker exec airflow_container dbt run --full-refresh \
  --project-dir /opt/airflow/dbt-transform \
  --profiles-dir /home/airflow/.dbt
```

**Bước 3.2: Kiểm định chất lượng (Data Quality Gates)**
Chạy 29 bài test tự động để đảm bảo dữ liệu không bị NULL sai chỗ, giá trị nằm trong chuẩn cho phép.
```bash
docker exec airflow_container dbt test \
  --project-dir /opt/airflow/dbt-transform \
  --profiles-dir /home/airflow/.dbt
```
> **Kết quả mong đợi:** `Done. PASS=29 WARN=0 ERROR=0 SKIP=0 TOTAL=29`

---

## GIAI ĐOẠN 4: KHAI THÁC GIAO DIỆN

Hệ thống cung cấp 2 giao diện quản trị chính:

### 1. Airflow UI (Điều phối tự động)
- **URL:** [http://localhost:8080](http://localhost:8080)
- **Đăng nhập:** Dùng biến `_AIRFLOW_WWW_USER_USERNAME` và `_AIRFLOW_WWW_USER_PASSWORD` trong file `.env`.
- **Tác vụ:** Bật (Unpause) DAG `open_meteo_api_pipeline_orchestrator` để hệ thống tự động chạy lấy dữ liệu real-time mỗi giờ (`@hourly`).

### 2. Superset UI (Phân tích & Trực quan)
- **URL:** [http://localhost:8088](http://localhost:8088)
- **Username:** `admin` (Mặc định cố định).
- **Password:** Dùng biến `SUPERSET_ADMIN_PASSWORD` trong file `.env`.

**Hướng Dẫn Kết Nối Superset Vào Kho Dữ Liệu:**
Sau khi đăng nhập Superset lần đầu, bạn cần trỏ nó vào Database:
1. Chuyển sang góc trên cùng bên phải, chọn **Settings** → **Database Connections** → nút **+ DATABASE**.
2. Chọn **PostgreSQL**.
3. Điền chuỗi kết nối (SQLAlchemy URI) sau đây:
   ```text
   postgresql+psycopg2://superset_user:<password>@postgres_db:5432/air_quality_db
   ```
   *(Thay `<password>` bằng giá trị của biến `SUPERSET_DB_PASSWORD` trong file `.env`)*
4. Nhấn **Test Connection**. Nếu hiện "Connection looks good!" thì bấm **Connect**.
5. Vào **SQL Lab**, thử chạy truy vấn kiểm tra dữ liệu:
   ```sql
   SELECT * FROM gold_layer.mart_hourly_conditions LIMIT 10;
   ```

---

## LỆNH QUẢN TRỊ NÂNG CAO

**1. Xem logs của các services để debug:**
```bash
docker compose logs airflow --tail=50
docker compose logs superset --tail=50
docker compose logs postgres_db --tail=50
```

**2. Xem dữ liệu trực tiếp trong Database qua CLI:**
```bash
docker exec -it postgres_container psql -U <POSTGRES_USER> -d air_quality_db \
  -c "SELECT COUNT(*) FROM gold_layer.mart_hourly_conditions;"
```

**3. Tạm dừng hệ thống (Giữ nguyên dữ liệu):**
```bash
docker compose stop
```

**4. Reset toàn bộ hệ thống từ con số 0 (Xóa sạch Data):**
Cực kỳ hữu ích khi bạn code sai và muốn làm lại môi trường sạch sẽ (Clean Slate).
```bash
docker compose down -v
docker compose up -d --build
```

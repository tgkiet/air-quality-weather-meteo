# Deep Dive: ELT Pipeline & Medallion Architecture

Tài liệu này giải thích từ gốc rễ mọi lý thuyết, quyết định thiết kế và kinh nghiệm thực chiến
của hệ thống Air Quality & Weather Data Platform — dành cho người học nghiêm túc muốn hiểu
**tại sao**, không chỉ **như thế nào**.

---

## 1. Tư Duy Nền Tảng: ETL vs ELT

### ETL (Mô hình cũ — trước 2015)

```
API → [Python Transform trên RAM] → Database (bảng đẹp)
```

- Transform xảy ra **trước khi** vào Database, trên bộ nhớ của máy chạy script.
- **Nhược điểm nghiêm trọng:** Nếu logic nghiệp vụ thay đổi (thêm cột, đổi công thức), bạn
  phải **gọi lại API từ đầu** để kéo dữ liệu về và Transform lại. Tốn tiền và thời gian.

### ELT (Mô hình hiện đại — dự án này)

```
API → Database (JSONB thô) → [SQL Transform bên trong DB] → Bảng đẹp
```

- Dữ liệu thô được lưu **vĩnh viễn** trong Database.
- Transform xảy ra **bên trong** Database bằng SQL (qua dbt).
- **Ưu điểm cốt lõi:** Sếp yêu cầu thêm cột `dew_point_level`? Bạn chỉ cần sửa file SQL và
  chạy `dbt run`. Không cần gọi lại API, không mất data.

**Điều kiện tiên quyết của ELT:** Database phải đủ mạnh để xử lý SQL phức tạp.
PostgreSQL với kiểu JSONB và các hàm `LATERAL`, `DISTINCT ON` đáp ứng hoàn hảo.

---

## 2. Medallion Architecture: Bronze → Silver → Gold

Databricks đề xướng mô hình 3 lớp này như một Industry Standard. Mỗi lớp có một
**hợp đồng dữ liệu (Data Contract)** rõ ràng.

### Tầng Bronze — "Bãi Đáp Thô"

**Nguyên tắc bất di bất dịch: Immutable (Bất biến) + Schema-on-read.**

Tại sao lưu JSONB thay vì tách ra từng cột ngay?

- Open-Meteo có thể thêm trường `solar_radiation` bất kỳ lúc nào.
- Nếu Bronze hardcode schema (có đúng 14 cột), một trường mới sẽ làm crash INSERT.
- Lưu JSONB = lưu nguyên vẹn toàn bộ payload. Khi cần trường mới, chỉ cần sửa SQL
  ở tầng Staging — không đụng Bronze.

**Idempotency (Tính Lũy Đẳng)** — khái niệm quan trọng nhất trong Data Engineering:

> Một thao tác là idempotent khi chạy 1 lần hay N lần đều cho cùng một kết quả.

Cách thực hiện: `ON CONFLICT (source_type, execution_date) DO UPDATE SET raw_json = EXCLUDED.raw_json`

- `(source_type, execution_date)` là UNIQUE CONSTRAINT vật lý trong DB.
- Airflow retry 3 lần? Không sao — cùng `execution_date` sẽ chỉ ghi đè, không tạo thêm dòng.
- Chạy Backfill lại từ đầu? Không sao — dữ liệu cũ bị đè bằng dữ liệu mới.

**Hai bảng Bronze trong hệ thống này:**

| Bảng | Nguồn | Constraint |
|---|---|---|
| `api_openmeteo_raw_data` | Airflow realtime (JSONB) | UNIQUE(source_type, execution_date) |
| `bronze_historical_weather` | CSV Hà Nội + API Backfill | UNIQUE(datetime, lat, lon) |

### Tầng Silver — "Nhà Máy Chế Biến"

**Nhiệm vụ chính: Flatten + Deduplicate + Union.**

**Kỹ thuật LATERAL UNNEST — trải phẳng JSONB:**

Open-Meteo trả về dữ liệu dạng mảng song song:
```json
{ "hourly": { "time": ["T00", "T01", ...], "temperature_2m": [27.1, 26.8, ...] } }
```

Để chuyển thành dạng bảng (1 dòng = 1 giờ), dùng `WITH ORDINALITY`:
```sql
LATERAL jsonb_array_elements_text(raw_json->'hourly'->'time') WITH ORDINALITY AS t(time_str, idx)
```
`idx` là số thứ tự (1, 2, 3...). Dùng `idx - 1` để lấy phần tử tương ứng trong mảng nhiệt độ.
**Đây là điểm dễ bug nhất:** PostgreSQL JSON array bắt đầu từ index 0, nhưng ORDINALITY bắt đầu từ 1.

**Kỹ thuật DISTINCT ON — loại trùng lặp:**

Khi Backfill và Airflow cùng có data cho ngày 21/05 lúc 14:00 tại HN Cầu Giấy:
```sql
SELECT DISTINCT ON (forecast_time, latitude, longitude)
    *
FROM (
    SELECT * FROM stg_weather_hourly      -- Airflow realtime
    UNION ALL
    SELECT * FROM stg_historical_weather  -- Backfill/CSV
)
ORDER BY forecast_time, latitude, longitude, execution_date DESC
```

- `DISTINCT ON`: Với mỗi tổ hợp `(time, lat, lon)`, chỉ giữ 1 dòng.
- `ORDER BY execution_date DESC`: Dòng được giữ lại là dòng có `execution_date` MỚI NHẤT.
- Kết quả: Airflow realtime (mới hơn) tự động thắng Backfill (cũ hơn). Mượt mà, tự động.

**UNION ALL Safety — thứ tự cột phải khớp:**

`UNION ALL` trong SQL ghép theo **vị trí cột (position)**, không theo tên cột.
```sql
-- ĐÚNG: cả hai SELECT phải có cùng thứ tự và số cột
SELECT raw_id, execution_date, latitude, longitude, location_name, forecast_time, temperature_2m, ...
FROM stg_weather_hourly
UNION ALL
SELECT raw_id, execution_date, latitude, longitude, location_name, forecast_time, temperature_2m, ...
FROM stg_historical_weather
```
Nếu `stg_historical_weather` đặt `location_name` ở vị trí khác → dữ liệu sẽ bị map sai cột hoàn toàn (data corruption thầm lặng, không có error). Đây là lý do tại sao các staging model **explicit list từng cột** thay vì dùng `SELECT *`.

**Incremental Materialization:**

dbt Silver dùng `materialized='incremental'`. Thay vì quét lại toàn bộ 4 năm dữ liệu mỗi lần Airflow chạy, nó chỉ xử lý batch MỚI:

```sql
{% if is_incremental() %}
WHERE execution_date > (SELECT COALESCE(MAX(execution_date), '1900-01-01') FROM {{ this }})
{% endif %}
```

Dùng `>` thay vì `>=` để tránh reprocess batch cuối (batch đó đã được xử lý rồi).

### Tầng Gold — "Quầy Phục Vụ"

**Nhiệm vụ: Denormalize + Enrich → Bảng phẳng cho BI.**

**Tại sao LEFT JOIN, không phải INNER JOIN?**

- Weather API trả về 7 ngày dự báo.
- Air Quality API (CAMS) chỉ có 5 ngày dự báo.
- INNER JOIN sẽ loại bỏ ngày 6-7 khỏi bảng Gold → mất data.
- LEFT JOIN giữ lại 7 ngày Weather, các cột AQ của ngày 6-7 = NULL (đúng hành vi, không phải lỗi).

**Tại sao JOIN trên `(forecast_time, lat, lon)` mà không có `location_name`?**

- Dữ liệu lịch sử Hà Nội từ CSV không có cột `location_name` (CSV cũ không có trường này).
- Nếu thêm `location_name` vào điều kiện JOIN: `NULL = NULL` trong SQL trả về `UNKNOWN` (không phải TRUE) → toàn bộ data lịch sử HN bị drop khỏi Gold.
- Giải pháp: Chỉ JOIN trên 3 cột số học. Vì Silver đã đảm bảo `(time, lat, lon)` là UNIQUE, phép JOIN này luôn là 1:1, không có Cartesian Fanout.

**NULL Safety trong CASE WHEN:**

```sql
-- SAI — nếu temperature_2m IS NULL, điều kiện >= 38 trả về NULL, không phải FALSE
CASE WHEN temperature_2m >= 38 THEN TRUE ELSE FALSE END AS is_weather_alert

-- ĐÚNG — guard IS NULL tường minh trước
CASE
    WHEN temperature_2m IS NULL THEN FALSE
    WHEN temperature_2m >= 38   THEN TRUE
    ELSE FALSE
END AS is_weather_alert
```

**NULL vs FALSE — triết lý quan trọng:**

`is_weather_alert` luôn TRUE/FALSE (không bao giờ NULL) vì Weather luôn có đủ 7 ngày data.
`is_air_quality_alert` có thể NULL vì AQ chỉ có 5 ngày. NULL ở đây có nghĩa "Chưa biết",
khác hoàn toàn với FALSE nghĩa "Không có alert". Dashboard phải hiển thị "N/A" thay vì tick xanh.

---

## 3. OOP & Clean Code trong Python

### Kiến Trúc Class Hierarchy

```
BasePostgresLoader
    ├── PostgresLoader   (UPSERT JSONB realtime)
    └── CSVLoader        (COPY EXPERT + UPSERT CSV)
    └── HistoricalBackfiller (Archive API + UPSERT)

OpenMeteoExtractor       (HTTP Session + Retry + Data Contract)
```

**Tính Kế Thừa (Inheritance):**
`BasePostgresLoader` chứa logic kết nối DB (connect, retry, close). Con cháu kế thừa và chỉ
cần viết thêm logic Insert/Load riêng của mình. Không lặp code kết nối DB ở nhiều nơi (DRY).

**Tính Đóng Gói (Encapsulation):**
`OpenMeteoExtractor` không biết DB tồn tại. `PostgresLoader` không biết HTTP tồn tại.
Mỗi class chỉ biết đúng phạm vi trách nhiệm của mình. Đây là nguyên lý **Single Responsibility**.

**Tách biệt Config và Code:**

```
config.json  ← 20 locations, API URLs, timeout, retry params
.env         ← DB password, Airflow password (không bao giờ commit Git)
.py files    ← Logic thuần túy, không có số ma thuật (magic number)
```

### COPY EXPERT — Tại Sao Không Dùng INSERT Thông Thường?

`INSERT INTO ... VALUES (...)` xử lý từng dòng một → 900,000 dòng CSV cần 900,000 round-trips DB.
`COPY EXPERT` stream toàn bộ file CSV qua binary protocol của PostgreSQL → nhanh hơn 10-50x.

Flow thực tế:
1. Tạo TEMP TABLE (không có constraint, không có index → tốc độ COPY tối đa).
2. COPY toàn bộ CSV vào TEMP TABLE.
3. UPSERT từ TEMP TABLE → bảng chính (có constraint xử lý trùng lặp).
4. TEMP TABLE tự động bị xóa khi transaction kết thúc (`ON COMMIT DROP`).

### Nearest-Neighbor Matching — Giải Quyết Grid Snapping

Open-Meteo API không nhận tọa độ chính xác — nó snap về grid cell gần nhất (~1km resolution).
Khi gửi batch 20 tọa độ, API trả về có thể ít hơn 20 items (các tọa độ gần nhau bị merge).

`_inject_location_metadata()` trong `main.py` giải quyết vấn đề này:
```python
# Với mỗi API response item (lat_api, lon_api):
# Tìm config location gần nhất trong bán kính 0.15°
dist = sqrt((lat_config - lat_api)^2 + (lon_config - lon_api)^2)
# Gán location_name của config location gần nhất
```

Kết quả: Mỗi API response item đều được gán đúng tên địa điểm từ config,
dù tọa độ thực tế của response có lệch so với tọa độ trong config.

---

## 4. Apache Airflow — Orchestration Layer

### Airflow không chạy code, nó lập lịch code

Airflow là "nhạc trưởng". Nó không biết thời tiết là gì. Nó chỉ biết:
- Lúc nào thì gọi `main.py`
- Lúc nào thì gọi `dbt run`
- Làm gì khi một task FAILED

### logical_date vs datetime.now() — Khái Niệm Cốt Lõi

**Tình huống:** Pipeline ngày 15/05 bị lỗi. Đến 20/05 bạn mới phát hiện và chạy lại.

| Cách dùng | Execution Date được dùng | Dữ liệu lấy về |
|---|---|---|
| `datetime.now()` | 20/05 | Data của ngày 20/05 — sai! |
| `logical_date` | 15/05 (cố định từ lúc schedule) | Data của ngày 15/05 — đúng! |

`logical_date` là thời điểm Airflow DỰ KIẾN chạy task (không phải thời điểm thực tế chạy).
Dù task bị retry lúc 10:05, `logical_date` vẫn giữ nguyên 10:00.
Đây là cơ chế đảm bảo Time-series data luôn chính xác và có thể backfill được.

### Fail Fast — Tại Sao Dùng `raise` Thay Vì `return`

```python
# SAI — che giấu lỗi, Airflow không biết có vấn đề
except Exception as e:
    logger.error(e)
    return None    # exit code = 0 → Airflow đánh SUCCESS

# ĐÚNG — báo lỗi cho Airflow
except Exception as e:
    logger.error(e)
    raise          # exit code != 0 → Airflow đánh FAILED → trigger Retry
```

### Task Dependency Chain

```
fetch_data >> dbt_run >> dbt_test
```

Nếu `fetch_data` FAILED → `dbt_run` và `dbt_test` bị SKIP.
Không bao giờ Transform dữ liệu rác khi bước Load thất bại.

### FabAuthManager vs SimpleAuthManager

Airflow 3 mặc định dùng `SimpleAuthManager` — chỉ có 1 user admin, password random.
Hệ thống này dùng `FabAuthManager` (Flask AppBuilder) vì:
- Nhiều users với RBAC (phân quyền theo role)
- Password có thể set qua CLI
- Hỗ trợ OAuth (Google, GitHub...)

---

## 5. dbt — Transform Layer

### dbt Làm Gì?

Bạn viết `SELECT`. dbt biến nó thành `CREATE TABLE AS SELECT` hoặc `CREATE VIEW AS SELECT`
tùy theo config `materialized`. Bạn không cần viết DDL thủ công.

### Materialization Strategy — Tại Sao Mỗi Layer Dùng Khác Nhau?

| Layer | Materialization | Lý Do |
|---|---|---|
| Staging | `view` | Không lưu vật lý. Staging chỉ là alias SQL. Thay đổi logic → chạy lại ngay, không cần full-refresh. |
| Silver | `incremental` | Chỉ process batch mới, không quét lại 4 năm dữ liệu mỗi giờ. Tốc độ tăng 10-100x. |
| Gold | `table` | BI Tool (Superset) đọc liên tục. TABLE truy vấn nhanh hơn VIEW vì không cần re-compute mỗi lần SELECT. |

### `generate_schema_name.sql` — Tại Sao Cần Macro Này?

Mặc định dbt tạo schema theo format: `<target_schema>_<folder_name>`.
Nếu `target.schema = public` và folder = `marts`, dbt tạo `public_marts` — xấu và khó quản lý.

Macro override này đảm bảo: folder `marts` → schema `gold_layer` (đúng như cấu hình).

### Data Quality Tests — 29 Bài Kiểm Tra Tự Động

```
dbt test chạy sau dbt run. Nếu bất kỳ test nào FAIL → Airflow task FAILED → Alert.
```

| Loại Test | Ý nghĩa | Ở đâu |
|---|---|---|
| `not_null` | Cột không được có giá trị NULL | Bronze, Silver, Gold |
| `unique` | Không có 2 dòng cùng giá trị | Bronze id, Silver (time,lat,lon) |
| `accepted_values` | Giá trị chỉ được nằm trong danh sách cho phép | Gold: temperature_level, uv_level, pm2_5_level |
| `not_null severity:warn` | NULL được phép nhưng cần cảnh báo | Silver/Gold: location_name (NULL với CSV HN cũ) |

---

## 6. PostgreSQL — Kiến Thức Nền Tảng

### JSONB vs JSON

| | JSON | JSONB |
|---|---|---|
| Lưu trữ | Text nguyên gốc | Binary đã parse |
| Tìm kiếm | Phải parse mỗi lần query | Đã index sẵn |
| Toán tử | Ít | Nhiều (`@>`, `?`, `#>`) |
| Hệ thống này dùng | Không | Có — vì cần query nhanh |

### UNIQUE Constraint vs Primary Key

- **Primary Key:** Tự động tạo index, không NULL, chỉ 1 PK mỗi bảng.
- **UNIQUE Constraint:** Có thể có nhiều, cho phép NULL (NULL != NULL trong SQL).
  Hệ thống này dùng UNIQUE Constraint trên `(source_type, execution_date)` để làm "mỏ neo"
  cho lệnh `ON CONFLICT DO UPDATE`.

### TIMESTAMPTZ vs TIMESTAMP

- `TIMESTAMP`: Lưu giờ địa phương, không có timezone info. Dễ nhầm múi giờ.
- `TIMESTAMPTZ`: Lưu UTC, tự động convert khi hiển thị theo timezone của client.

Hệ thống này dùng `TIMESTAMPTZ` cho tất cả các cột thời gian. Open-Meteo trả về giờ địa phương
(`Asia/Bangkok`, UTC+7), nên cần explicit convert:
```sql
CAST(time_str AS TIMESTAMP) AT TIME ZONE 'Asia/Bangkok'
```
PostgreSQL hiểu đây là giờ Bangkok và lưu về UTC (trừ đi 7 tiếng).

---

## 7. Docker — Tại Sao Và Như Thế Nào

### Tại Sao Cần Docker?

Không Docker: "Code chạy được trên máy tôi" nhưng crash trên máy khác (khác Python version,
khác thư viện, khác hệ điều hành).

Docker: Đóng gói toàn bộ code + dependencies + OS layers vào 1 container image bất biến.
Chạy ở đâu cũng giống nhau.

### Kiến Trúc Container Trong Dự Án Này

```
docker-compose.yml quản lý 2 services:

postgres_container
  - Image: postgres:16
  - Databases: air_quality_db (data), airflow_db (Airflow metadata)
  - Mount: init_dbs.sh → /docker-entrypoint-initdb.d/ (chạy 1 lần khi khởi tạo)

airflow_container
  - Image: custom (Dockerfile: python3.13 + Airflow 3.2.0 + dbt-postgres 1.9.0)
  - Depends on: postgres_container (healthcheck)
  - Mount: src/, dbt-transform/, profiles.yml (read-only), Open-Meteo-Dataset/
  - Network: meteo_network (nội bộ, Airflow gọi DB bằng hostname "postgres_db")
```

**Healthcheck** đảm bảo Airflow chỉ start SAU KHI PostgreSQL thực sự sẵn sàng nhận kết nối.
Không có healthcheck → race condition → Airflow crash vì DB chưa ready.

**Volume mount `profiles.yml` read-only (`ro`):**
- Ngăn Airflow container vô tình ghi đè file credentials.
- Best practice bảo mật: container chỉ đọc, không ghi file nhạy cảm.

---

## 8. Kinh Nghiệm Thực Chiến — Những Bài Học Đắt Giá

**API Grid Snapping:** Đừng assume API trả về đúng số location bạn gửi. Luôn log số response
trả về và so sánh với số request. Phát hiện sớm bằng warning log trong `_inject_location_metadata`.

**Positional Bug trong UNION ALL:** Đây là loại bug thầm lặng nhất — không có error, chỉ có data
sai. Quy tắc: LUÔN explicit list cột trong cả 2 vế của UNION ALL. Không dùng `SELECT *`.

**NULL trong SQL không phải False:** `NULL >= 55` trả về `NULL`, không phải `FALSE`. `NULL OR FALSE`
trả về `NULL`. Luôn guard `IS NULL` tường minh trước các so sánh số.

**Timezone phải explicit:** "Asia/Bangkok" phải xuất hiện cả trong Python (backfill) lẫn SQL
(staging model). Thiếu một nơi → data join sai vì giờ lệch 7 tiếng.

**Idempotency phải ở tầng DB, không chỉ ở code:** Code check trùng lặp trước khi Insert có thể
bị race condition (2 process chạy đồng thời). UNIQUE CONSTRAINT ở DB là lớp bảo vệ vật lý,
luôn đúng kể cả khi có race condition.

**`raise` thay vì `return None`:** Mọi exception ở tầng Extract/Load đều phải `raise`. Che giấu
lỗi bằng `return None` là nguyên nhân số 1 của các bug khó debug trong pipeline.

---

## 9. Bản Đồ Đọc Tài Liệu

| Thứ tự | File | Nên đọc để hiểu |
|---|---|---|
| 1 | `weather-meteo-data-platform/README.md` | Kiến trúc tổng thể, luồng data, Quick Start |
| 2 | `weather-meteo-data-platform/src/README.md` | OOP design, Extractor, Loader, Backfill strategy |
| 3 | `weather-meteo-data-platform/dbt-transform/README.md` | LATERAL unnest, DISTINCT ON, JOIN 1:1, 29 tests |
| 4 | `weather-meteo-data-platform/airflow/README.md` | DAG config, logical_date, FabAuthManager |
| 5 | `weather-meteo-data-platform/docs/ARCHITECTURE.md` | System diagram, component interactions |
| 6 | `weather-meteo-data-platform/docs/SETUP.md` | Hướng dẫn chạy từng bước, env variables |
| 7 | `weather-meteo-data-platform/docs/TROUBLESHOOTING.md` | Các lỗi thường gặp và cách fix |

---

## 10. Checklist Trước Khi Chuyển Sang Superset

- [ ] Hiểu tại sao dùng JSONB ở Bronze (Schema-on-read)
- [ ] Hiểu `ON CONFLICT DO UPDATE` đảm bảo Idempotency như thế nào
- [ ] Hiểu `DISTINCT ON` + `ORDER BY execution_date DESC` giải quyết xung đột data
- [ ] Hiểu tại sao JOIN trên `(time, lat, lon)` chứ không có `location_name`
- [ ] Hiểu `logical_date` vs `datetime.now()` trong Airflow
- [ ] Hiểu tại sao `raise` quan trọng hơn `return None`
- [ ] Hiểu `is_air_quality_alert = NULL` khác `= FALSE`
- [ ] Đã chạy xong 5 bước trong `lenhdocker.txt`
- [ ] `dbt test` trả về `PASS=29 WARN=0 ERROR=0`
- [ ] Bảng `gold_layer.mart_hourly_conditions` có dữ liệu

# dbt Transform — Lớp Biến Đổi Dữ Liệu (Silver & Gold Layers)

Thư mục này chứa toàn bộ logic biến đổi dữ liệu (Transform) trong kiến trúc ELT. Nhiệm vụ chính là "nhào nặn" cục JSON thô từ tầng Bronze thành các bảng dữ liệu có cấu trúc (Tabular) chuẩn chỉnh phục vụ phân tích.

---

## Kiến Trúc & Cấu Hình (OOP Mindset)

Hệ thống dbt được thiết kế dựa trên nguyên tắc **Tách biệt cấu hình và mã nguồn** (Separation of Concerns):

1. **`requirements_airflow.txt`**: Cài đặt trực tiếp `dbt-postgres` vào bên trong Airflow (không dùng Docker Socket, không cần dbt_container dư thừa).
2. **`../.dbt/profiles.yml`**: Nơi lưu cấu hình kết nối Database. Tuyệt đối không hardcode mật khẩu, mọi kết nối được truyền qua biến môi trường (`{{ env_var('POSTGRES_USER') }}`) nạp từ file `.env`.
3. **`dbt_project.yml`**: Trung tâm điều khiển dự án. Áp dụng cấu hình Materialization theo hướng đối tượng (Folder-based):
   - Mọi model trong `staging/` tự động biến thành `VIEW` ảo (Tiết kiệm dung lượng).
   - Mọi model trong `silver/` tự động biến thành `TABLE` vật lý với cơ chế `incremental` để tối ưu hiệu suất đọc ghi.
   - Mọi model trong `marts/` tự động biến thành `TABLE` vật lý và được xếp vào schema riêng biệt `gold_layer`.
4. **Macro Override (`generate_schema_name.sql`)**: Ghi đè hành vi sinh tên schema mặc định của dbt (tránh schema xấu kiểu `silver_layer_gold_layer`), đảm bảo Tầng Gold nằm đúng ở schema `gold_layer` chuẩn chỉnh.

---

## Cấu Trúc Lớp Dữ Liệu (Medallion Architecture)

### 1. Tầng Khai Báo Nguồn (Sources)
- **Vị trí:** `models/staging/sources.yml`
- **Mục đích:** Loại bỏ việc hardcode tên schema/bảng trong câu lệnh SQL (`SELECT * FROM public.api_...`).
- Cung cấp tính năng test đầu vào tự động (`unique`, `not_null`) và dựng phả hệ dữ liệu (Data Lineage) chuẩn xác.

### 2. Tầng Tiền Xử Lý (Staging Layer)
- **Vị trí:** `models/staging/`
- **Nhiệm vụ:** Lấy dữ liệu từ nguồn và bóc tách (Flatten) các mảng JSON song song phức tạp của API Open-Meteo bằng tuyệt kỹ PostgreSQL: `LATERAL jsonb_array_elements_text(...) WITH ORDINALITY`.
- **Tư Duy Tối Ưu (Enterprise Mindset):**
  - **Micro-Optimization:** Đưa các hàm `ROUND()` để làm tròn tọa độ lên CTE trên cùng để chỉ chạy 1 lần/dòng.
  - **Timezone Shift Prevention:** Ép chuẩn `AT TIME ZONE 'Asia/Bangkok'` về `TIMESTAMPTZ` để Postgres chuyển đổi chuẩn xác múi giờ địa phương về giờ UTC gốc.
  - **Float Jitter Prevention:** Làm tròn tọa độ 2 chữ số thập phân để chống nứt gãy Khóa Chính ở tầng Silver.

### 3. Tầng Tinh Chế (Silver Layer)
- **Vị trí:** `models/silver/`
- **Nhiệm vụ:** Ghi dữ liệu vật lý với cơ chế **Incremental** (Tính Lũy đẳng). Chỉ đọc và ghi những bản ghi "mới xuất hiện", chống quá tải database.
- **Tư Duy Tối Ưu (Enterprise Mindset):**
  - **Data Duplication Prevention:** Dùng tuyệt kỹ `DISTINCT ON (forecast_time, ...)` kết hợp `ORDER BY execution_date DESC` để lọc bỏ các bản dự báo bị đè lên nhau giữa các lần gọi API. Tránh sập lệnh `MERGE` của dbt.
  - **Idempotency:** Sử dụng `execution_date >= max(...)` để luôn bao quát các mẻ chạy Retry hoặc Backfill của Airflow mà không sinh ra rác dữ liệu.

### 4. Tầng Thương Mại (Gold Layer / Data Marts)
- **Vị trí:** `models/marts/`
- **Nhiệm vụ:** Giải chuẩn hóa (Denormalize) dữ liệu, gộp bảng Thời tiết và Không khí thành một Bảng Phẳng (Flat Table) duy nhất, tính toán sẵn các chỉ số (Derived Metrics) để Data Analyst và Superset chỉ việc kéo-thả (Kéo 1 phát ăn ngay).
- **Tư Duy Tối Ưu (Enterprise Mindset):**
  - **Join Strategy (LEFT JOIN):** Chọn Bảng Thời Tiết (dự báo 7 ngày) làm Bảng Trụ Cột (Base), ghép thêm dữ liệu Không Khí (dự báo 5 ngày) vào. Bảo toàn tối đa giá trị cốt lõi của thời tiết mà không làm mất dữ liệu của ngày 6 và ngày 7.
  - **Graceful Degradation (NULL Handling):** Khi Không Khí bị NULL ở ngày 6-7, các cột tính toán (như Cờ Cảnh Báo Không Khí) được thiết kế theo nguyên lý **NULL-safe**, trả về chuỗi "Chưa có dữ liệu" hoặc `NULL` thay vì kết luận sai lệch là `FALSE` (An toàn).
  - **RBAC (Role-Based Access Control):** Tách bạch Tầng Gold ra schema `gold_layer` riêng biệt, giúp dễ dàng phân quyền cho Data Analyst (chỉ được phép đọc `gold_layer`, không thấy `silver_layer`).

---

## Hệ Thống Kiểm Duyệt Chất Lượng (Data Quality Gates)

Pipeline hiện tại sở hữu **17 bài Data Tests** tự động, tạo thành lưới bảo vệ nhiều lớp:
- **Bronze (4 tests):** Đảm bảo ID dữ liệu thô không trùng lặp, Không có dòng nào thiếu Execution Date.
- **Silver (6 tests):** Khóa chính (Tọa độ + Thời gian) tuyệt đối không được phép NULL.
- **Gold (7 tests):** Không chỉ test khóa chính, mà còn test các cột Derived Labels (như Cờ cảnh báo thời tiết) phải luôn có giá trị để đảm bảo Dashboard hoạt động chính xác.

---

## Hướng Dẫn Thực Hành / Chạy Thủ Công

Dbt hiện tại đã được **tích hợp sâu vào trong Airflow Container** để đảm bảo 100% sự đồng nhất giữa môi trường Dev và Prod (Tránh lỗi *Environment Drift*). Do đó, không có `dbt_container` riêng lẻ nào cả.

Để thực hành hoặc debug thủ công, bạn cần chui vào máy Airflow:

**1. Truy cập vào Airflow Container:**
```bash
docker exec -it airflow_container bash
```

**2. Chạy biến đổi dữ liệu (Transform):**
Lệnh này sẽ compile các file `.sql` và chạy chúng trong PostgreSQL.
```bash
dbt run --project-dir /opt/airflow/dbt-transform --profiles-dir /home/airflow/.dbt
```
*(Mẹo: Thêm cờ `--full-refresh` nếu bạn thay đổi cấu trúc bảng).*

**3. Chạy kiểm duyệt dữ liệu (Data Tests):**
```bash
dbt test --project-dir /opt/airflow/dbt-transform --profiles-dir /home/airflow/.dbt
```

---

## Luồng Chạy Tự Động (Airflow Orchestration)

Khi Airflow DAG `open_meteo_api_pipeline_orchestrator` kích hoạt, nó sẽ tự động gọi chuỗi lệnh sau:
1. `fetch_data`: Python Extract & Load (Kéo JSON đẩy vào Bronze).
2. `dbt_run`: Kích hoạt Staging -> Silver -> Gold (Transform & Load).
3. `dbt_test`: Chạy 17 Data Tests để đóng mộc chứng nhận chất lượng. Tự động đánh FAIL toàn bộ Pipeline nếu dữ liệu bị lỗi.

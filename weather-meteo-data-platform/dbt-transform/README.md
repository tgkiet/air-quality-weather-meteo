# dbt Transform — Lớp Biến Đổi Dữ Liệu (Silver & Gold Layers)

Thư mục này chứa toàn bộ logic biến đổi dữ liệu (Transform) trong kiến trúc ELT. Nhiệm vụ chính là "nhào nặn" cục JSON thô từ tầng Bronze thành các bảng dữ liệu có cấu trúc (Tabular) chuẩn chỉnh phục vụ phân tích.

---

## Kiến Trúc & Cấu Hình (OOP Mindset)

Hệ thống dbt được thiết kế dựa trên nguyên tắc **Tách biệt cấu hình và mã nguồn** (Separation of Concerns):

1. **`docker-compose.yml`**: Khởi chạy dbt trong một container biệt lập (`dbt_container`) ở chế độ chạy ngầm (`tail -f /dev/null`). Điều này giữ cho môi trường dbt luôn đồng nhất và "sạch".
2. **`../.dbt/profiles.yml`**: Nơi lưu cấu hình kết nối Database. Tuyệt đối không hardcode mật khẩu, mọi kết nối được truyền qua biến môi trường (`{{ env_var('POSTGRES_USER') }}`) nạp từ file `.env`.
3. **`dbt_project.yml`**: Trung tâm điều khiển dự án. Áp dụng cấu hình Materialization theo hướng đối tượng (Folder-based):
   - Mọi model trong `staging/` tự động biến thành `VIEW` ảo (Tiết kiệm dung lượng).
   - Mọi model trong `silver/` tự động biến thành `TABLE` vật lý (Hoặc Incremental để tối ưu hiệu suất đọc ghi).

---

## Cấu Trúc Lớp Dữ Liệu (Medallion Architecture)

### 1. Tầng Khai Báo Nguồn (Sources)
- **Vị trí:** `models/staging/sources.yml`
- **Mục đích:** Loại bỏ việc hardcode tên schema/bảng trong câu lệnh SQL (`SELECT * FROM public.api_...`).
- Cung cấp tính năng test đầu vào tự động (`unique`, `not_null`) và dựng phả hệ dữ liệu (Data Lineage) chuẩn xác.

### 2. Tầng Tiền Xử Lý (Staging Layer)
- **Vị trí:** `models/staging/`
- **Các file hiện tại:** `stg_weather_hourly.sql`, `stg_air_quality_hourly.sql`
- **Nhiệm vụ:**
  - Lấy dữ liệu từ nguồn qua hàm `{{ source('meteo_bronze', 'api_openmeteo_raw_data') }}`.
  - Bóc tách (Flatten) các mảng JSON song song phức tạp của API Open-Meteo bằng tuyệt kỹ PostgreSQL: `LATERAL jsonb_array_elements_text(...) WITH ORDINALITY`.
  - Ép kiểu dữ liệu (Casting) từ chuỗi sang `NUMERIC`, `TIMESTAMP`.
  - **Quy tắc Vàng:** KHÔNG thực hiện JOIN giữa các bảng logic khác nhau tại tầng này. Chỉ tập trung làm sạch một nguồn duy nhất. Tối ưu performance bằng cách gộp mọi logic bóc tách vào một CTE duy nhất.

### 3. Tầng Tinh Chế (Silver Layer - Đang phát triển)
- **Vị trí:** `models/silver/` (Sắp tạo)
- **Nhiệm vụ:** Ghi dữ liệu vật lý với cơ chế **Incremental** (Tính Lũy đẳng). Chỉ đọc và ghi những bản ghi "mới xuất hiện" ở tầng Staging, không Full-Refresh quét lại toàn bộ dữ liệu lịch sử để chống quá tải database.

---

## Hướng Dẫn Vận Hành Trực Tiếp

Vì dbt chạy trong Docker, bạn cần dùng lệnh `docker exec` để "chui" vào container và ra lệnh.

**1. Kiểm tra kết nối:**
Kiểm tra xem file `profiles.yml` đã móc nối thành công vào PostgreSQL chưa.
```bash
docker exec -it dbt_container dbt debug
```

**2. Chạy toàn bộ luồng biến đổi:**
Lệnh này sẽ compile các file `.sql` thành mã SQL chuẩn của Postgres và chạy chúng trong database.
```bash
docker exec -it dbt_container dbt run
```

**3. Chạy kiểm duyệt dữ liệu (Data Tests):**
Khởi chạy các bài test đã cài đặt trong file `sources.yml`.
```bash
docker exec -it dbt_container dbt test
```

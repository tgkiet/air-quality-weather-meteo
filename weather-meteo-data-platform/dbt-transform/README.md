# dbt Transform — Lớp Biến Đổi Dữ Liệu (Silver & Gold Layers)

Thư mục này chứa toàn bộ logic biến đổi dữ liệu (Transform) trong kiến trúc ELT. Nhiệm vụ chính là "nhào nặn" cục JSON thô từ tầng Bronze thành các bảng dữ liệu có cấu trúc (Tabular) chuẩn chỉnh phục vụ phân tích.

---

## Kiến Trúc & Cấu Hình (OOP Mindset)

Hệ thống dbt được thiết kế dựa trên nguyên tắc **Tách biệt cấu hình và mã nguồn** (Separation of Concerns):

1. **`docker-compose.yml`**: Khởi chạy dbt trong một container biệt lập (`dbt_container`) ở chế độ chạy ngầm (`tail -f /dev/null`). Điều này giữ cho môi trường dbt luôn đồng nhất và "sạch".
2. **`../.dbt/profiles.yml`**: Nơi lưu cấu hình kết nối Database. Tuyệt đối không hardcode mật khẩu, mọi kết nối được truyền qua biến môi trường (`{{ env_var('POSTGRES_USER') }}`) nạp từ file `.env`.
3. **`dbt_project.yml`**: Trung tâm điều khiển dự án. Áp dụng cấu hình Materialization theo hướng đối tượng (Folder-based):
   - Mọi model trong `staging/` tự động biến thành `VIEW` ảo (Tiết kiệm dung lượng).
   - Mọi model trong `silver/` tự động biến thành `TABLE` (cụ thể là `incremental` để tối ưu hiệu suất đọc ghi).

---

## Cấu Trúc Lớp Dữ Liệu (Medallion Architecture)

### 1. Tầng Khai Báo Nguồn (Sources)
- **Vị trí:** `models/staging/sources.yml`
- **Mục đích:** Loại bỏ việc hardcode tên schema/bảng trong câu lệnh SQL (`SELECT * FROM public.api_...`).
- Cung cấp tính năng test đầu vào tự động (`unique`, `not_null`) và dựng phả hệ dữ liệu (Data Lineage) chuẩn xác.

### 2. Tầng Tiền Xử Lý (Staging Layer)
- **Vị trí:** `models/staging/`
- **Nhiệm vụ:**
  - Lấy dữ liệu từ nguồn qua hàm `{{ source(...) }}`.
  - Bóc tách (Flatten) các mảng JSON song song phức tạp của API Open-Meteo bằng tuyệt kỹ PostgreSQL: `LATERAL jsonb_array_elements_text(...) WITH ORDINALITY`.
- **Tư Duy Tối Ưu (Enterprise Mindset):**
  - **Micro-Optimization:** Đưa các hàm `ROUND()` để làm tròn tọa độ lên CTE trên cùng để chỉ chạy 1 lần/dòng, tránh bị lặp lại 24 lần trong vòng lặp `LATERAL`.
  - **Timezone Shift Prevention:** Không dùng `TIMESTAMP` thuần túy. Bắt buộc dùng `AT TIME ZONE 'Asia/Bangkok'` và ép về `TIMESTAMPTZ` để Postgres chuyển đổi chuẩn xác múi giờ địa phương về giờ UTC gốc.
  - **Float Jitter Prevention:** Làm tròn tọa độ 2 chữ số thập phân để chống nứt gãy Khóa Chính ở tầng Silver do API dao động ngầm.

### 3. Tầng Tinh Chế (Silver Layer)
- **Vị trí:** `models/silver/`
- **Nhiệm vụ:** Ghi dữ liệu vật lý với cơ chế **Incremental** (Tính Lũy đẳng). Chỉ đọc và ghi những bản ghi "mới xuất hiện", không Full-Refresh quét lại toàn bộ dữ liệu lịch sử để chống quá tải database.
- **Tư Duy Tối Ưu (Enterprise Mindset):**
  - **Data Duplication Prevention:** Dùng tuyệt kỹ `DISTINCT ON (forecast_time, ...)` kết hợp `ORDER BY execution_date DESC` để lọc bỏ các bản dự báo bị đè lên nhau giữa các lần gọi API. Nếu không có bước này, lệnh `MERGE` của dbt sẽ sập toàn hệ thống!
  - **Idempotency:** Sử dụng `execution_date >= max(...)` để luôn bao quát các mẻ chạy Retry hoặc Backfill của Airflow mà không sinh ra rác dữ liệu.
  - **Zero-Trust Testing:** Thiết lập file `models/silver/schema.yml` để áp dụng luật Test `not_null` lên 100% các cột Khóa Chính, tuyệt đối không tin tưởng dữ liệu đầu vào.

---

## Hướng Dẫn Vận Hành Trực Tiếp

Vì dbt chạy trong Docker, bạn cần dùng lệnh `docker exec` để "chui" vào container và ra lệnh.

**1. Kiểm tra kết nối:**
```bash
docker exec -it dbt_container dbt debug
```

**2. Chạy toàn bộ luồng biến đổi:**
Lệnh này sẽ compile các file `.sql` thành mã SQL chuẩn của Postgres và chạy chúng trong database.
```bash
docker exec -it dbt_container dbt run
```
*(Mẹo: Thêm cờ `--full-refresh` nếu bạn thay đổi cấu trúc bảng hoặc muốn dbt xóa bảng cũ xây lại từ đầu).*

**3. Chạy kiểm duyệt dữ liệu (Data Tests):**
Khởi chạy hàng rào bảo vệ chất lượng dữ liệu (10 bài test ở Bronze & Silver).
```bash
docker exec -it dbt_container dbt test
```

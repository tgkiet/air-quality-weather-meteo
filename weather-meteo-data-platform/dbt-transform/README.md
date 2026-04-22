# dbt Transform (Transform Layer)

Thư mục này chứa dự án dbt (Data Build Tool), chịu trách nhiệm cho **Bước T (Transform)** trong luồng ELT.

## Vai trò
Sau khi dữ liệu thô (JSONB) đã được tải vào bảng Bronze (`api_openmeteo_raw_data`) bởi quy trình Extract & Load, dbt sẽ chạy các script SQL để nhào nặn dữ liệu:
1. **Lọc trùng (Deduplication):** Sử dụng các window function (như `ROW_NUMBER()`) để loại bỏ các bản ghi bị trùng do tính chất Append-Only của bảng raw.
2. **Bóc tách (Parsing):** Dùng toán tử JSON của PostgreSQL (`->>`, `->`) để trải các key/value từ JSONB ra thành các cột độc lập (Tabular Format).
3. **Làm sạch (Cleansing):** Đổi kiểu dữ liệu (Date, Float, Integer), đổi tên cột, xử lý giá trị null.
4. **Marts:** Join bảng dữ liệu Thời tiết và Chất lượng không khí lại với nhau thành một bảng phân tích dùng để vẽ Dashboard BI.

## Cấu trúc chuẩn dự kiến
- **`models/staging/`**: Tầng Silver, bóc tách JSON và làm sạch cơ bản.
- **`models/marts/`**: Tầng Gold, tổng hợp và chuẩn bị cho Reporting.
- **`dbt_project.yml`**: File cấu hình dự án dbt.

## Lưu ý
(Dự án hiện đang ở bước setup cơ bản, các model của dbt sẽ được phát triển ở giai đoạn sau của dự án).

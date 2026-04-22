# Source Code (Extract & Load Layer)

Thư mục `src/` chứa toàn bộ logic lập trình bằng Python để thực hiện **Bước E (Extract)** và **Bước L (Load)** trong mô hình ELT. Các module ở đây được viết theo hướng OOP (Lập trình hướng đối tượng), đảm bảo tính tái sử dụng và độc lập hoàn toàn với Airflow.

## Kiến Trúc Bên Trong
- **`extractors/`**: 
  - Đảm nhận nhiệm vụ **Extract**.
  - `open_meteo.py`: Chứa class `OpenMeteoExtractor` chuyên kết nối và kéo dữ liệu JSON từ các endpoint của Open-Meteo (Weather API và Air Quality API) kèm theo cơ chế retry nếu lỗi.
- **`loaders/`**: 
  - Đảm nhận nhiệm vụ **Load**.
  - `postgres_loader.py`: Chứa class `PostgresLoader` quản lý kết nối an toàn với PostgreSQL và thực hiện lệnh `INSERT INTO` để nhét cục raw JSON vào Database. Nó sử dụng `psycopg2.sql` để **ngăn chặn hoàn toàn lỗi bảo mật SQL Injection** khi truyền tên bảng.
- **`scripts/`**: 
  - Chứa các đoạn script setup ban đầu. Nổi bật là `init_dbs.sh`, file bash script tự động chạy khi khởi tạo PostgreSQL container để thiết lập song song 2 Database riêng biệt (tách biệt Airflow Metadata và Data Warehouse) nhằm đảm bảo an toàn bảo mật.
- **`utils/`**: 
  - Chứa các hàm tiện ích dùng chung cho toàn bộ pipeline.
  - `logger.py`: Cung cấp class Logger được chuẩn hoá, giúp ghi log tất cả hoạt động ra màn hình console và đồng thời lưu vào file `logs/pipeline.log`. Mọi sự cố và thao tác đều được lưu vết chi tiết.
- **`main.py`**:
  - Entrypoint chạy độc lập. Nó đóng vai trò "dán keo" Lớp Extract và Lớp Load lại với nhau để bạn có thể chạy thử nghiệm ở Local mà không cần Airflow.

## Luồng Dữ Liệu (Data Flow):

1. **Extract Thời tiết (`https://api.open-meteo.com/v1/forecast`)**: 
   Truyền vào tham số `hourly` để lấy dữ liệu theo từng giờ bao gồm:
   - Nhiệt độ, độ ẩm, điểm sương, nhiệt độ cảm nhận.
   - Tỷ lệ có mưa, lượng mưa.
   - Áp suất khí quyển, độ phủ mây, tầm nhìn.
   - Tốc độ gió, hướng gió, gió giật.
   - Chỉ số tia UV.

2. **Extract Chất lượng không khí (`https://air-quality-api.open-meteo.com/v1/air-quality`)**: 
   Lấy thêm một loạt dữ liệu theo giờ về các hạt bụi và khí thải:
   - Bụi mịn PM10, PM2.5, Bụi thường (Dust).
   - Khí CO (Carbon Monoxide), NO2 (Nitrogen Dioxide), SO2 (Sulphur Dioxide), Ozone.
   - Aerosol optical depth (độ đục quang học của không khí do sol khí).

3. **Load Database (`PostgresLoader`)**: 
   Sau khi `extractor` lấy thành công cả 2 cục dữ liệu JSON bự kể trên, Lớp `loader` sẽ gọi hàm `insert_data` 2 lần để tống cả 2 cục này vào chung một bảng `api_openmeteo_raw_data` dưới dạng chuỗi string (Kiểu `JSONB`).
   Để dễ dàng phân tách và Transform sau này, chúng được đánh dấu phân biệt bằng cột `source_type`:
   - `weather_forecast_hourly`
   - `air_quality_hourly`

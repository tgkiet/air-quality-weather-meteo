# Air Quality & Weather Data Pipeline Project

Chào mừng đến với dự án **Air Quality & Weather Data Pipeline**. Đây là một hệ thống Data Engineering end-to-end hoàn chỉnh nhằm mục đích thu thập, xử lý và phân tích dữ liệu Thời tiết, Khí tượng và Chất lượng không khí (Air Quality) tại các khu vực TPHCM.

## Mục Tiêu Dự Án
Xây dựng một Data Platform tự động hóa quy trình kéo dữ liệu từ API, lưu trữ, làm sạch và tổng hợp dữ liệu phục vụ cho BI & Analytics theo chuẩn Data Engineering hiện đại (Modern Data Stack).

## Kiến Trúc Hệ Thống (Medallion Architecture)
Dự án áp dụng chặt chẽ mô hình **ELT** (Extract - Load - Transform) kết hợp với **Medallion Architecture**:

1. Bronze (Raw Data):
   - **Mô tả:** Dữ liệu JSON thô hỗn tạp kéo trực tiếp từ Open-Meteo API.
   - **Lưu trữ:** Bảng `api_openmeteo_raw_data` trong PostgreSQL (cột `raw_json` kiểu JSONB).
   - **Phân loại:** Dựa vào cột `source_type` (`weather_forecast_hourly`, `air_quality_hourly`).

2. Silver (Cleaned & Transformed Data):
   - **Mô tả:** Dữ liệu JSON đã được làm sạch, bóc tách thành các cột (tabular format), cast đúng kiểu dữ liệu (Date, Float, Int) và xử lý null.
   - **Công cụ:** `dbt` (Data Build Tool).

3. Gold (Business Level Data):
   - **Mô tả:** Dữ liệu đã được join (kết hợp) giữa Thời tiết và Chất lượng không khí, được tổng hợp theo ngày/tháng/vùng để sẵn sàng đưa lên Dashboard báo cáo.

## Cấu Trúc Thư Mục
Dự án được chia làm các module chính (Đi sâu vào từng thư mục để đọc README chi tiết):

- [`weather-meteo-data-platform/`](./weather-meteo-data-platform/README.md): Chứa toàn bộ core logic của hệ thống Data Pipeline (Airflow, dbt, Python scripts, Docker).
- `Open-Meteo-Dataset/`: Chứa bộ dữ liệu crawl mẫu (về air quality và weather & meteo từ 02/8/2022 đến 29/11/2025 tại 31 location từ OpenAQ). Dữ liệu này được gitignore do dung lượng quá lớn. *Liên hệ để lấy file data raw.*

## Liên Hệ (Contact)
- **Email:** giakiettran14102005@gmail.com


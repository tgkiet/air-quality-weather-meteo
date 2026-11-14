# Dashboard dự báo mẫu

Gói [weather-dashboard.zip](./weather-dashboard.zip) có sáu biểu đồ và hai bộ lọc cho thành phố và điểm tra cứu. Dữ liệu PM2.5 dùng µg/m³. Gói mẫu kết nối tới một database riêng với quyền SELECT. File [sample.sql](./sample.sql) tạo 200 dòng giả lập theo giờ hiện tại, gồm một điểm mỗi thành phố và một khoảng thiếu PM2.5 để kiểm tra cách hiển thị.

Từ thư mục `weather-meteo-data-platform`, trên một stack local đã khởi động, tạo database mẫu và role mẫu:

```bash
docker compose exec -T postgres_db sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
CREATE ROLE meteo_demo LOGIN PASSWORD 'demo';
CREATE DATABASE meteo_demo;
REVOKE CONNECT ON DATABASE meteo_demo FROM PUBLIC;
GRANT CONNECT ON DATABASE meteo_demo TO meteo_demo;
SQL

docker compose exec -T postgres_db sh -c \
  'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d meteo_demo' < superset/demo/sample.sql
```

Trong Superset, mở **Dashboards → Import**, chọn `weather-dashboard.zip`, rồi nhập mật khẩu mẫu `demo` nếu được hỏi. Chọn **HN** hoặc **HCM** để kiểm tra từng thành phố. Các biểu đồ dùng giờ Việt Nam. Chạy lại fixture trên database mẫu mới khi muốn đặt lại mốc thời gian.

Để dùng dữ liệu thật, cập nhật kết nối dataset sang warehouse bằng tài khoản chỉ đọc Gold. Bảng Gold cần được dựng từ phiên bản dbt có các cột thời điểm ingest. Ghi rõ nguồn Open-Meteo và CAMS Global khi chia sẻ dashboard. Các giá trị trong demo được tạo bằng SQL, nên chỉ dùng để kiểm tra giao diện.

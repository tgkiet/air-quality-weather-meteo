# Sổ Tay Khai Thác Dữ Liệu & Trực Quan Hóa (Superset Playbook)

Tài liệu này là cẩm nang bắt buộc dành cho Data Analyst / BI Developer trước khi bắt tay vào xây dựng Dashboard trên hệ thống Air Quality & Weather Data Platform bằng Apache Superset.

## 1. Cấu hình Bắt Buộc: Đồng bộ Timezone cho Bộ Lọc (Filters)
Kho dữ liệu PostgreSQL lưu trữ thời gian ở chuẩn quốc tế (`TIMESTAMPTZ` - UTC). Để các bộ lọc thời gian của Superset (như "Today", "Last 7 days") tự động nhận diện đúng múi giờ Việt Nam khi truy vấn, bạn phải cấu hình ép múi giờ ở phần Connection.

**Cách thực hiện:**
1. Trong màn hình Edit Database Connection của Superset, chuyển sang tab **Advanced**.
2. Mở rộng mục **Other**.
3. Tại ô **ENGINE PARAMETERS**, dán cấu hình sau:
```json
{
  "connect_args": {
    "options": "-c timezone=Asia/Bangkok"
  }
}
```

---

## 2. Bước Đệm: Khởi Tạo Dataset
Trong Superset, để vẽ được biểu đồ, bạn không thể truy vấn thẳng vào Database mà phải đăng ký bảng đó thành một **Dataset**.
1. Trên thanh công cụ trên cùng, chọn **Datasets**.
2. Bấm nút **+ DATASET** ở góc phải.
3. Chọn theo thứ tự:
   *   **Database:** `PostgreSQL` (Đây là tên mặc định của connection bạn vừa tạo).
   *   **Schema:** `gold_layer`
   *   **Table:** `mart_hourly_conditions`
4. Bấm **CREATE DATASET AND CREATE CHART** để bắt đầu hành trình.

---

## 3. Các Nguyên Tắc Vàng Khi Vẽ Biểu Đồ

Để làm chủ bảng `gold_layer.mart_hourly_conditions`, hãy tuân thủ nghiêm ngặt 4 nguyên tắc sau:

### Nguyên tắc 3.1: Chọn đúng Cột Thời Gian (Time Column)
Hệ thống có 2 cột mang định dạng thời gian. Bạn **tuyệt đối phải chọn `forecast_time`** làm trục X (Time Column) cho mọi biểu đồ Time-series.
*   ✅ **`forecast_time`**: Điểm thời gian thực tế mà thời tiết/không khí diễn ra (Quá khứ, Hiện tại, hoặc Tương lai).
    > 💡 **Bí mật kỹ thuật:** Cột này đã được tự động ép về múi giờ Việt Nam (Naive Local Time) ngay từ tầng Data Warehouse để vô hiệu hóa lỗi lệch 7 tiếng của thư viện ECharts trên Superset. Bạn cứ yên tâm vẽ mà không lo bị lệch giờ!
*   ❌ **`execution_date`**: Chỉ là metadata nội bộ, ghi nhận thời điểm con bot Airflow chạy lệnh kéo dữ liệu.

### Nguyên tắc 3.2: Luôn dùng Hàm `AVG` thay vì `SUM`
Khi bạn vẽ biểu đồ Nhiệt độ, Tia UV, hay lượng bụi PM2.5 theo khu vực (`location_name`), ở phần Metric **bắt buộc phải chọn Hàm `AVG` (Trung bình)**. Đừng bao giờ dùng `SUM`.
*   **Lý do kỹ thuật (Historical Mapping Artifact):** Trong quá trình backfill lịch sử, thuật toán đã gộp nhiều trạm đo vật lý (nhiều tọa độ khác nhau) vào chung một khu vực đại diện (Ví dụ: `HN Cầu Giấy` có thể chứa dữ liệu của 10 sensor khác nhau trong cùng 1 khung giờ). 
*   Việc dùng `AVG` sẽ giúp tính trung bình chính xác cho cả khu vực. Nếu dùng `SUM`, nhiệt độ của 10 sensor sẽ bị cộng dồn lên tới hàng trăm độ C!

### Nguyên tắc 3.3: Tận dụng sức mạnh của các Cột Phân Loại (Categorical)
Thay vì bắt người xem tự phân tích các con số thô (ví dụ: PM2.5 = 150 µg/m³ là tốt hay xấu?), hãy tận dụng các cột dbt đã dọn sẵn ở Tầng Gold để vẽ **Pie Chart** hoặc **Bảng đếm**:

| Cột Phân Loại | Ý nghĩa / Cách sử dụng |
| :--- | :--- |
| `pm2_5_level` | Chuyển đổi chỉ số bụi mịn thành Text ("Tốt", "Trung bình", "Kém", "Nguy hiểm") dựa trên chuẩn WHO. Rất hợp để vẽ Pie Chart tỷ lệ ô nhiễm. |
| `temperature_level` | Đánh giá mức độ khắc nghiệt của thời tiết ("Mát mẻ", "Nóng", "Rất nóng", "Nguy hiểm"). |
| `is_weather_alert` | Cờ Boolean (`True`/`False`). Bật `True` khi có mưa to, gió giật mạnh, hoặc không khí ô nhiễm nặng. Rất hợp dùng làm thẻ KPI đếm số giờ báo động. |

### Nguyên tắc 3.4: Đừng hoảng hốt với "Độ trễ Không khí" (AQ Lag)
Khi bạn vẽ biểu đồ dự báo 7 ngày tương lai, bạn sẽ thấy cột Nhiệt độ/Mưa trải dài liền mạch cả tuần. Tuy nhiên, các cột chỉ số không khí (như `pm2_5`, `aqi`) sẽ hiển thị `NULL` ở ngày thứ 6 và thứ 7.
*   **Bản chất:** Đây là giới hạn thực tế của các thiết bị đo môi trường trên toàn cầu. API CAMS (Copernicus Atmosphere Monitoring Service) chỉ có thể dự báo chất lượng không khí tối đa 5 ngày.
*   **Kết luận:** Đây không phải lỗi rớt mạng hay mất dữ liệu. Hãy thiết kế Dashboard khéo léo (ví dụ: chú thích rõ khoảng dự báo AQI chỉ là 5 ngày).

---

## 4. Câu Query SQL Lab Khởi Động
Để có cái nhìn tổng quan nhất về dữ liệu đã được làm sạch, hãy chạy đoạn SQL này trong SQL Lab của Superset:

```sql
SELECT 
    forecast_time, 
    location_name, 
    temperature_2m, 
    pm2_5_level, 
    is_weather_alert 
FROM gold_layer.mart_hourly_conditions 
ORDER BY forecast_time DESC
LIMIT 50;
```
Đoạn truy vấn này bốc ra ngay những cột "tinh túy" nhất, chứng minh giá trị của hệ thống ETL đằng sau. Chúc bạn vẽ Dashboard thành công!

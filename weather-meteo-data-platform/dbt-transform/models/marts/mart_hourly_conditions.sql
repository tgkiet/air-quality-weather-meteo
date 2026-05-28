-- ============================================================
-- TẦNG GOLD: Data Mart - mart_hourly_conditions
-- ============================================================
-- MỤC ĐÍCH:
--   Gộp (Denormalize) 2 bảng Silver thành 1 bảng phẳng duy nhất
--   để Data Analyst và Superset có thể kéo-thả mà không cần JOIN.
--
-- CHIẾN THUẬT JOIN: LEFT JOIN
--   - Bảng TRÁI (Base)  : slv_weather_hourly     → Luôn đủ 7 ngày
--   - Bảng PHẢI (Enrich): slv_air_quality_hourly → Chỉ có ~5 ngày
--   - Kết quả: 7 ngày Thời tiết đầy đủ. Ngày 6-7 cột Không khí = NULL
--     (đây là hành vi đúng, không phải lỗi — xem README để hiểu rõ)
--
-- MATERIALIZATION: Được cấu hình là TABLE trong dbt_project.yml
--   (Không khai báo lại ở đây — tuân thủ nguyên tắc DRY/OOP)
--
--
-- SCHEMA: Được cấu hình là gold_layer trong dbt_project.yml
--   (Tách biệt khỏi silver_layer để phân quyền RBAC)
-- ============================================================

WITH weather AS (
    -- Lấy toàn bộ dữ liệu Thời tiết từ Silver.
    -- Dùng ref() để dbt tự vẽ Data Lineage và kiểm soát thứ tự chạy.
    SELECT * FROM {{ ref('slv_weather_hourly') }}
),

air_quality AS (
    -- Lấy toàn bộ dữ liệu Chất lượng không khí từ Silver.
    SELECT * FROM {{ ref('slv_air_quality_hourly') }}
),

joined AS (
    SELECT
        -- ==========================================
        -- NHÓM 1: TRƯỜNG ĐỊNH DANH (Dimensions)
        -- Xác định "Dữ liệu này là của giờ nào, ở đâu"
        -- ==========================================
        -- BI TIMEZONE HACK (Quan trọng):
        -- Ép TIMESTAMPTZ (chuẩn UTC) về lại Naive Timestamp (giờ địa phương Việt Nam).
        -- Mục đích: Vô hiệu hóa lỗi lệch 7 tiếng của ECharts trên Superset (ECharts luôn ép
        -- trục X render theo UTC). Khi truyền Naive Timestamp, Superset buộc phải hiển thị 
        -- đúng chữ số giờ của Việt Nam.
        w.forecast_time AT TIME ZONE 'Asia/Bangkok' AS forecast_time,
        w.latitude,
        w.longitude,
        w.execution_date AT TIME ZONE 'Asia/Bangkok' AS execution_date,
        -- w.location_name hiển thị tên vùng quan trắc (HN Quận Cầu Giấy, HCM Quận 1...)
        -- cho 52 locations (30 HN + 22 HCM).
        w.location_name,

        -- ==========================================
        -- NHÓM 2: CHỈ SỐ THỜI TIẾT (Weather Metrics)
        -- Nguồn: slv_weather_hourly — luôn đủ 7 ngày
        -- ==========================================
        w.temperature_2m,
        w.relative_humidity_2m,
        w.dew_point_2m,
        w.apparent_temperature,
        w.precipitation_probability,
        w.precipitation,
        w.pressure_msl,
        w.surface_pressure,
        w.cloud_cover,
        w.visibility,
        w.wind_speed_10m,
        w.wind_direction_10m,
        w.wind_gusts_10m,
        w.uv_index                      AS weather_uv_index,

        -- ==========================================
        -- NHÓM 3: CHỈ SỐ CHẤT LƯỢNG KHÔNG KHÍ (Air Quality Metrics)
        -- Nguồn: slv_air_quality_hourly — chỉ có ~5 ngày
        -- Các cột này có thể NULL ở ngày 6-7 (ngoài tầm dự báo CAMS)
        -- ==========================================
        aq.pm10,
        aq.pm2_5,
        aq.carbon_monoxide,
        aq.nitrogen_dioxide,
        aq.sulphur_dioxide,
        aq.ozone,
        aq.aerosol_optical_depth,
        aq.dust,
        aq.uv_index                     AS aq_uv_index,

        -- ==========================================
        -- NHÓM 4: CÁC CỘT ĐƯỢC TÍNH TOÁN (Derived Columns)
        -- "Giá trị gia tăng" của Tầng Gold — Data Analyst không cần tự tính.
        -- Nguyên tắc NULL-safe: Luôn kiểm tra IS NULL TRƯỚC các điều kiện số
        -- để tránh lỗi logic ngầm trong SQL (NULL >= N trả về NULL, không phải FALSE)
        -- ==========================================

        -- 4a. Phân loại Nhiệt độ (Health Risk Level)
        --     NULL-safe: weather luôn có dữ liệu, nhưng vẫn guard để phòng
        CASE
            WHEN w.temperature_2m IS NULL THEN 'Chưa có dữ liệu'
            WHEN w.temperature_2m >= 40   THEN 'Nguy hiểm'
            WHEN w.temperature_2m >= 35   THEN 'Rất nóng'
            WHEN w.temperature_2m >= 30   THEN 'Nóng'
            WHEN w.temperature_2m >= 20   THEN 'Dễ chịu'
            ELSE                               'Mát mẻ'
        END                             AS temperature_level,

        -- 4b. Phân loại Chỉ số UV (Chuẩn WHO)
        --     NULL-safe: tương tự temperature
        CASE
            WHEN w.uv_index IS NULL THEN 'Chưa có dữ liệu'
            WHEN w.uv_index >= 11   THEN 'Cực kỳ nguy hiểm'
            WHEN w.uv_index >= 8    THEN 'Rất cao'
            WHEN w.uv_index >= 6    THEN 'Cao'
            WHEN w.uv_index >= 3    THEN 'Trung bình'
            ELSE                         'Thấp'
        END                             AS uv_level,

        -- 4c. Phân loại PM2.5 (Tiêu chuẩn AQI Mỹ - phổ biến nhất)
        --     NULL-safe: pm2_5 có thể NULL ở ngày 6-7
        CASE
            WHEN aq.pm2_5 IS NULL THEN 'Chưa có dữ liệu'
            WHEN aq.pm2_5 >= 150  THEN 'Nguy hiểm'
            WHEN aq.pm2_5 >= 55   THEN 'Rất không lành mạnh'
            WHEN aq.pm2_5 >= 35   THEN 'Không lành mạnh'
            WHEN aq.pm2_5 >= 12   THEN 'Trung bình'
            ELSE                       'Tốt'
        END                             AS pm2_5_level,

        -- 4d. Cờ Cảnh báo Thời tiết (Weather Alert Flag)
        --     LUÔN trả về TRUE/FALSE, KHÔNG BAO GIỜ NULL (phù hợp với not_null test).
        --     (1) Nhiệt độ >= 38°C → TRUE (temperature luôn có dữ liệu 7 ngày)
        --            (2) UV >= 8 VÀ UV không NULL → TRUE (guard UV NULL tường minh)
        --            (3) Mọi trường hợp còn lại → FALSE
        --     Không dùng `uv_index >= 8 OR temperature >= 38` trực tiếp vì:
        --     NULL >= 8 → NULL, và NULL OR FALSE → NULL → ELSE bắt → FALSE (ẩn)
        --     Cách viết tường minh dưới đây không có NULL ngầm ở bất kỳ nhánh nào.
        CASE
            WHEN w.temperature_2m >= 38                          THEN TRUE
            WHEN w.uv_index IS NOT NULL AND w.uv_index >= 8      THEN TRUE
            ELSE                                                      FALSE
        END                             AS is_weather_alert,

        -- 4e. Cờ Cảnh báo Không khí (Air Quality Alert Flag)
        --     NULL-safe: Trả về NULL (thay vì FALSE) khi chưa có dữ liệu.
        --     Lý do: FALSE ngầm hiểu là "Không khí tốt" — SAI khi thực ra là "Chưa biết".
        --     Dashboard nên hiển thị "N/A" thay vì dấu tick xanh khi giá trị là NULL.
        CASE
            WHEN aq.pm2_5 IS NULL THEN NULL  -- Chưa có dự báo, không kết luận
            WHEN aq.pm2_5 >= 55   THEN TRUE  -- Ô nhiễm nặng → Cảnh báo
            ELSE                        FALSE -- Trong ngưỡng an toàn
        END                             AS is_air_quality_alert

    FROM weather AS w
    -- LEFT JOIN: Giữ lại toàn bộ 7 ngày Thời tiết.
    -- Không khí ghép vào khi có (~5 ngày), để NULL khi không có.
    -- JOIN THEO KHÓA CHÍNH CỦA SILVER LAYER: (forecast_time, latitude, longitude)
    -- LÝ DO CHỈ DÙNG 3 CỘT NÀY:
    -- 1. Bảng Silver (slv_weather, slv_aq) đã dùng `DISTINCT ON (forecast_time, lat, lon)`
    --    đảm bảo tuyệt đối bộ 3 cột này là Khóa Chính (Primary Key).
    -- 2. Join trên bộ 3 Khóa Chính đảm bảo toán học 100% là phép JOIN 1:1,
    --    hoàn toàn không có khả năng xảy ra Cartesian Fanout (nhân bản dòng).
    -- 3. Chỉ sử dụng Khóa chính toán học, không phụ thuộc vào `location_name`.
    LEFT JOIN air_quality AS aq
        ON  w.forecast_time = aq.forecast_time
        AND w.latitude      = aq.latitude
        AND w.longitude     = aq.longitude
)

SELECT * FROM joined
-- Xóa ORDER BY — không có tác dụng khi materialized='table'.
-- PostgreSQL không đảm bảo physical storage order, và SELECT sử sau không
-- kế thừa ORDER BY này. Để tăng tốc query, tạo index thay thế:
--   CREATE INDEX ON mart_hourly_conditions(forecast_time);
--   CREATE INDEX ON mart_hourly_conditions(latitude, longitude);
-- Hai index này có thể được thêm vào dưới dạng dbt post-hook trong schema.yml.

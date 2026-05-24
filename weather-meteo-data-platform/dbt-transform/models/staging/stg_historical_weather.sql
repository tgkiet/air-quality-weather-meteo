-- CRIT-2 FIX: Liệt kê rõ TỪNG CỘT với đúng thứ tự khớp stg_weather_hourly.sql
-- UNION ALL khớp cột theo VỊ TRÍ, không theo tên. Nếu thứ tự sai,
-- ví dụ location_name bị map vào forecast_time → data corrupt hoàn toàn.
-- Luôn explicit SELECT thay vì SELECT * cho 2 bên của UNION ALL.

WITH source_data AS (
    SELECT
        id AS raw_id,
        -- LOGIC-3 NOTE: ingested_at được dùng thành execution_date vì historical data
        -- không chạy qua Airflow nên không có logical_date. Ý nghĩa: "thời điểm
        -- dữ liệu này được nạp vào hệ thống", không phải "thời điểm Airflow lên lịch".
        -- Silver model dùng execution_date để sort priority — càng mới càng ưu tiên.
        -- Realtime data luôn có execution_date > historical data → đương đúng.
        ingested_at                               AS execution_date,
        ROUND(CAST(lat AS NUMERIC), 4)            AS latitude,
        ROUND(CAST(lon AS NUMERIC), 4)            AS longitude,
COALESCE(
            location_name,
            CASE 
                WHEN CAST(location_id AS INT) = 2161318 THEN 'HN Cầu Giấy'
                WHEN CAST(location_id AS INT) = 4946812 THEN 'HN Hà Đông Đông'
                WHEN CAST(location_id AS INT) = 2161307 THEN 'HN Cầu Giấy'
                WHEN CAST(location_id AS INT) = 2161301 THEN 'HN Hà Đông Đông'
                WHEN CAST(location_id AS INT) = 2161293 THEN 'HN Chương Mỹ'
                WHEN CAST(location_id AS INT) = 2161303 THEN 'HN Hoàng Mai'
                WHEN CAST(location_id AS INT) = 2161299 THEN 'HN Long Biên'
                WHEN CAST(location_id AS INT) = 4946811 THEN 'HN Long Biên'
                WHEN CAST(location_id AS INT) = 7441 THEN 'HN Cầu Giấy'
                WHEN CAST(location_id AS INT) = 2539 THEN 'HN Cầu Giấy'
                WHEN CAST(location_id AS INT) = 2161321 THEN 'HN Hà Đông Đông'
                WHEN CAST(location_id AS INT) = 2161304 THEN 'HN Cầu Giấy'
                WHEN CAST(location_id AS INT) = 2161290 THEN 'HN Nam Từ Liêm Tây'
                WHEN CAST(location_id AS INT) = 2161295 THEN 'HN Hoàng Mai'
                WHEN CAST(location_id AS INT) = 2161296 THEN 'HN Long Biên'
                WHEN CAST(location_id AS INT) = 2161306 THEN 'HN Bắc Từ Liêm Tây'
                WHEN CAST(location_id AS INT) = 2161300 THEN 'HN Hoàng Mai'
                WHEN CAST(location_id AS INT) = 2161308 THEN 'HN Cầu Giấy'
                WHEN CAST(location_id AS INT) = 2161313 THEN 'HN Hoàng Mai'
                WHEN CAST(location_id AS INT) = 2161291 THEN 'HN Bắc Từ Liêm Tây'
                WHEN CAST(location_id AS INT) = 4946813 THEN 'HN Hoàng Mai'
                WHEN CAST(location_id AS INT) = 2161294 THEN 'HN Long Biên'
                WHEN CAST(location_id AS INT) = 2161320 THEN 'HN Đông Anh'
                WHEN CAST(location_id AS INT) = 2161298 THEN 'HN Long Biên'
                WHEN CAST(location_id AS INT) = 2161316 THEN 'HN Cầu Giấy'
                WHEN CAST(location_id AS INT) = 2161309 THEN 'HN Hoàng Mai Nam'
                WHEN CAST(location_id AS INT) = 2161292 THEN 'HN Cầu Giấy'
                WHEN CAST(location_id AS INT) = 2161315 THEN 'HN Nam Từ Liêm Tây'
                WHEN CAST(location_id AS INT) = 2161322 THEN 'HN Bắc Từ Liêm'
                WHEN CAST(location_id AS INT) = 2161314 THEN 'HN Cầu Giấy'
                WHEN CAST(location_id AS INT) = 1285357 THEN 'HN Cầu Giấy'
                ELSE 'HN Station ' || CAST(CAST(location_id AS INT) AS VARCHAR)
            END
        ) AS location_name,
        datetime                                  AS forecast_time,
        temperature_2m,
        relative_humidity_2m,
        CAST(NULL AS NUMERIC)                     AS dew_point_2m,
        CAST(NULL AS NUMERIC)                     AS apparent_temperature,
        CAST(NULL AS NUMERIC)                     AS precipitation_probability,
        precipitation,
        pressure_msl,
        CAST(NULL AS NUMERIC)                     AS surface_pressure,
        CAST(NULL AS NUMERIC)                     AS cloud_cover,
        CAST(NULL AS NUMERIC)                     AS visibility,
        wind_speed_10m,
        wind_direction_10m,
        CAST(NULL AS NUMERIC)                     AS wind_gusts_10m,
        CAST(NULL AS NUMERIC)                     AS uv_index
    FROM {{ source('meteo_bronze', 'bronze_historical_weather') }}
)

-- Thứ tự cột này phải KHỚP CHÍNH XÁC với stg_weather_hourly.sql
-- vì Silver model dùng UNION ALL (position-based, không phải name-based):
-- raw_id, execution_date, latitude, longitude, location_name, forecast_time,
-- temperature_2m, relative_humidity_2m, dew_point_2m, apparent_temperature,
-- precipitation_probability, precipitation, pressure_msl, surface_pressure,
-- cloud_cover, visibility, wind_speed_10m, wind_direction_10m, wind_gusts_10m, uv_index
SELECT
    raw_id,
    execution_date,
    latitude,
    longitude,
    location_name,
    forecast_time,
    temperature_2m,
    relative_humidity_2m,
    dew_point_2m,
    apparent_temperature,
    precipitation_probability,
    precipitation,
    pressure_msl,
    surface_pressure,
    cloud_cover,
    visibility,
    wind_speed_10m,
    wind_direction_10m,
    wind_gusts_10m,
    uv_index
FROM source_data

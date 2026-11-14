-- Liệt kê rõ TỪNG CỘT với đúng thứ tự khớp stg_weather_hourly.sql
-- UNION ALL khớp cột theo VỊ TRÍ, không theo tên. Nếu thứ tự sai,
-- ví dụ location_name bị map vào forecast_time → data corrupt hoàn toàn.
-- Luôn explicit SELECT thay vì SELECT * cho 2 bên của UNION ALL.

WITH source_data AS (
    SELECT
        id AS raw_id,
        -- ingested_at được dùng thành execution_date vì historical data
        -- không chạy qua Airflow nên không có logical_date. Ý nghĩa: "thời điểm
        -- dữ liệu này được nạp vào hệ thống", không phải "thời điểm Airflow lên lịch".
        ingested_at                               AS execution_date,
        ROUND(CAST(lat AS NUMERIC), 4)            AS latitude,
        ROUND(CAST(lon AS NUMERIC), 4)            AS longitude,
        COALESCE(location_name, 'Unknown Station ' || CAST(location_id AS VARCHAR)) AS location_name,
        datetime                                  AS forecast_time,
        temperature_2m,
        relative_humidity_2m,
        dew_point_2m,
        apparent_temperature,
        CAST(NULL AS NUMERIC)                     AS precipitation_probability,
        precipitation,
        pressure_msl,
        surface_pressure,
        cloud_cover,
        CAST(NULL AS NUMERIC)                     AS visibility,
        wind_speed_10m,
        wind_direction_10m,
        wind_gusts_10m,
        uv_index,
        ingested_at
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
    uv_index,
    ingested_at,
    'historical'::text AS data_source,
    NULL::numeric AS model_latitude,
    NULL::numeric AS model_longitude
FROM source_data

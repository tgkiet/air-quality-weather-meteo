-- Khai báo Materialization: Tầng Staging thường dùng 'view' để tiết kiệm dung lượng,
-- dữ liệu vật lý chỉ được tạo ra ở các tầng sau (Silver/Gold).
{{ config(
    materialized='view'
) }}

WITH source_data AS (
    SELECT 
        id as raw_id,
        execution_date,
        raw_json
    FROM {{ source('meteo_bronze', 'api_openmeteo_raw_data') }}
    WHERE source_type = 'weather_forecast_hourly'
),

extracted_arrays AS (
    SELECT 
        raw_id,
        execution_date,
        -- Lấy tọa độ
        CAST(raw_json->>'latitude' AS NUMERIC) AS latitude,
        CAST(raw_json->>'longitude' AS NUMERIC) AS longitude,
        
        -- Ép kiểu chuỗi ISO thành Timestamp
        CAST(t.time_str AS TIMESTAMP) AS forecast_time,
        
        -- Dùng idx để "móc" dữ liệu ở các mảng khác tương ứng cùng vị trí
        -- Lưu ý: Index của PostgreSQL jsonb array bắt đầu từ 0, nên phải lấy idx - 1
        CAST(raw_json->'hourly'->'temperature_2m'->>(t.idx::int - 1) AS NUMERIC) AS temperature_2m,
        CAST(raw_json->'hourly'->'relative_humidity_2m'->>(t.idx::int - 1) AS NUMERIC) AS relative_humidity_2m,
        CAST(raw_json->'hourly'->'dew_point_2m'->>(t.idx::int - 1) AS NUMERIC) AS dew_point_2m,
        CAST(raw_json->'hourly'->'apparent_temperature'->>(t.idx::int - 1) AS NUMERIC) AS apparent_temperature,
        CAST(raw_json->'hourly'->'precipitation_probability'->>(t.idx::int - 1) AS NUMERIC) AS precipitation_probability,
        CAST(raw_json->'hourly'->'precipitation'->>(t.idx::int - 1) AS NUMERIC) AS precipitation,
        CAST(raw_json->'hourly'->'pressure_msl'->>(t.idx::int - 1) AS NUMERIC) AS pressure_msl,
        CAST(raw_json->'hourly'->'surface_pressure'->>(t.idx::int - 1) AS NUMERIC) AS surface_pressure,
        CAST(raw_json->'hourly'->'cloud_cover'->>(t.idx::int - 1) AS NUMERIC) AS cloud_cover,
        CAST(raw_json->'hourly'->'visibility'->>(t.idx::int - 1) AS NUMERIC) AS visibility,
        CAST(raw_json->'hourly'->'wind_speed_10m'->>(t.idx::int - 1) AS NUMERIC) AS wind_speed_10m,
        CAST(raw_json->'hourly'->'wind_direction_10m'->>(t.idx::int - 1) AS NUMERIC) AS wind_direction_10m,
        CAST(raw_json->'hourly'->'wind_gusts_10m'->>(t.idx::int - 1) AS NUMERIC) AS wind_gusts_10m,
        CAST(raw_json->'hourly'->'uv_index'->>(t.idx::int - 1) AS NUMERIC) AS uv_index
        
    FROM source_data,
    LATERAL jsonb_array_elements_text(raw_json->'hourly'->'time') WITH ORDINALITY AS t(time_str, idx)
)

SELECT * FROM extracted_arrays

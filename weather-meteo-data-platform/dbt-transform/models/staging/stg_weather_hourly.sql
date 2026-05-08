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
        
        -- Bung mảng 'time' ra thành nhiều dòng, kèm theo số thứ tự (index)
        -- WITH ORDINALITY sẽ tạo ra cột 'idx' (bắt đầu từ 1, 2, 3...)
        t.time_str,
        t.idx
    FROM source_data,
    LATERAL jsonb_array_elements_text(raw_json->'hourly'->'time') WITH ORDINALITY AS t(time_str, idx)
),

final_flattened AS (
    SELECT 
        e.raw_id,
        e.execution_date,
        e.latitude,
        e.longitude,
        
        -- Ép kiểu chuỗi ISO thành Timestamp
        CAST(e.time_str AS TIMESTAMP) AS forecast_time,
        
        -- Dùng lại idx để "móc" dữ liệu ở các mảng khác tương ứng cùng vị trí
        -- Lưu ý: Index của PostgreSQL jsonb array bắt đầu từ 0, nên phải lấy idx - 1
        CAST(s.raw_json->'hourly'->'temperature_2m'->>(e.idx::int - 1) AS NUMERIC) AS temperature_2m,
        CAST(s.raw_json->'hourly'->'relative_humidity_2m'->>(e.idx::int - 1) AS NUMERIC) AS relative_humidity_2m,
        CAST(s.raw_json->'hourly'->'precipitation_probability'->>(e.idx::int - 1) AS NUMERIC) AS precipitation_probability,
        CAST(s.raw_json->'hourly'->'wind_speed_10m'->>(e.idx::int - 1) AS NUMERIC) AS wind_speed_10m
        
    FROM extracted_arrays e
    JOIN source_data s ON e.raw_id = s.raw_id
)

SELECT * FROM final_flattened

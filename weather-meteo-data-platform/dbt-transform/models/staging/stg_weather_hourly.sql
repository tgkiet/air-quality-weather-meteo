

WITH raw_records AS (
    SELECT
        id AS raw_id,
        CAST(execution_date AS TIMESTAMPTZ) AS execution_date,
        single_json
    FROM {{ source('meteo_bronze', 'api_openmeteo_raw_data') }}
    CROSS JOIN LATERAL (
        SELECT 
            CASE 
                WHEN jsonb_typeof(raw_json) = 'array' THEN raw_json
                ELSE jsonb_build_array(raw_json)
            END AS arr
    ) AS array_wrapper
    CROSS JOIN LATERAL jsonb_array_elements(array_wrapper.arr) AS single_json
    WHERE source_type = 'weather_forecast_hourly'
),

source_data AS (
    SELECT 
        raw_id,
        execution_date,
        CASE 
            WHEN single_json->>'requested_latitude' IS NOT NULL 
                THEN ROUND(CAST(single_json->>'requested_latitude' AS NUMERIC), 4)
            WHEN ROUND(CAST(single_json->>'latitude' AS NUMERIC), 1) = 10.8 
                THEN 10.78
            ELSE ROUND(CAST(single_json->>'latitude' AS NUMERIC), 4)
        END AS latitude,
        CASE 
            WHEN single_json->>'requested_longitude' IS NOT NULL 
                THEN ROUND(CAST(single_json->>'requested_longitude' AS NUMERIC), 4)
            WHEN ROUND(CAST(single_json->>'longitude' AS NUMERIC), 1) = 106.7 
                THEN 106.70
            ELSE ROUND(CAST(single_json->>'longitude' AS NUMERIC), 4)
        END AS longitude,
        single_json AS raw_json
    FROM raw_records
),

extracted_arrays AS (
    SELECT 
        raw_id,
        execution_date,
        latitude,
        longitude,
        
        -- LỖI TIMEZONE ĐÃ ĐƯỢC FIX: Open-Meteo trả về giờ địa phương (Asia/Bangkok).
        -- Nếu chỉ dùng TIMESTAMP, BI Tool sẽ hiểu nhầm là giờ UTC.
        -- Phải dùng AT TIME ZONE để Postgres dịch chuẩn xác về TIMESTAMPTZ (UTC base).
        CAST(t.time_str AS TIMESTAMP) AT TIME ZONE 'Asia/Bangkok' AS forecast_time,
        
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



WITH source_data AS (
    SELECT 
        id as raw_id,
        CAST(execution_date AS TIMESTAMPTZ) AS execution_date,
        ROUND(CAST(raw_json->>'latitude' AS NUMERIC), 2) AS latitude,
        ROUND(CAST(raw_json->>'longitude' AS NUMERIC), 2) AS longitude,
        raw_json
    FROM {{ source('meteo_bronze', 'api_openmeteo_raw_data') }}
    WHERE source_type = 'air_quality_hourly'
),

extracted_arrays AS (
    SELECT 
        raw_id,
        execution_date,
        latitude,
        longitude,
        
        -- Ép chuẩn Timezone để đồng bộ với execution_date (tránh lỗi ngầm khi Join)
        CAST(t.time_str AS TIMESTAMP) AT TIME ZONE 'Asia/Bangkok' AS forecast_time,
        
        -- Dùng idx để "móc" dữ liệu ở các mảng khác tương ứng cùng vị trí
        -- Lưu ý: Index của PostgreSQL jsonb array bắt đầu từ 0, nên phải lấy idx - 1
        CAST(raw_json->'hourly'->'pm10'->>(t.idx::int - 1) AS NUMERIC) AS pm10,
        CAST(raw_json->'hourly'->'pm2_5'->>(t.idx::int - 1) AS NUMERIC) AS pm2_5,
        CAST(raw_json->'hourly'->'carbon_monoxide'->>(t.idx::int - 1) AS NUMERIC) AS carbon_monoxide,
        CAST(raw_json->'hourly'->'nitrogen_dioxide'->>(t.idx::int - 1) AS NUMERIC) AS nitrogen_dioxide,
        CAST(raw_json->'hourly'->'sulphur_dioxide'->>(t.idx::int - 1) AS NUMERIC) AS sulphur_dioxide,
        CAST(raw_json->'hourly'->'ozone'->>(t.idx::int - 1) AS NUMERIC) AS ozone,
        CAST(raw_json->'hourly'->'aerosol_optical_depth'->>(t.idx::int - 1) AS NUMERIC) AS aerosol_optical_depth,
        CAST(raw_json->'hourly'->'dust'->>(t.idx::int - 1) AS NUMERIC) AS dust,
        CAST(raw_json->'hourly'->'uv_index'->>(t.idx::int - 1) AS NUMERIC) AS uv_index
        
    FROM source_data,
    LATERAL jsonb_array_elements_text(raw_json->'hourly'->'time') WITH ORDINALITY AS t(time_str, idx)
)

SELECT * FROM extracted_arrays

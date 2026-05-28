
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
    WHERE source_type = 'air_quality_hourly'
),

source_data AS (
    SELECT 
        raw_id,
        execution_date,

        -- Xóa magic number fallback (10.8→10.78, 106.7→106.70) vì chúng
        -- là dead code sau khi _inject_location_metadata() đã inject requested_latitude
        -- vào toàn bộ items. Nhánh WHEN IS NOT NULL luôn đúng → 2 WHEN còn lại
        -- không bao giờ chạy tới. Giữ lại gây nhầm lẫn cho người đọc sau.
        ROUND(CAST(single_json->>'requested_latitude'  AS NUMERIC), 4) AS latitude,
        ROUND(CAST(single_json->>'requested_longitude' AS NUMERIC), 4) AS longitude,

        -- Trích xuất location_name từ raw_json để surface lên Silver & Gold.
        -- Trước đây location_name được inject vào JSONB nhưng không bao giờ được đọc ra.
        -- Không có location_name → Dashboard chỉ thấy tọa độ số, không biết tên quận/trạm.
        single_json->>'location_name' AS location_name,

        single_json AS raw_json
    FROM raw_records
),

extracted_arrays AS (
    SELECT 
        raw_id,
        execution_date,
        latitude,
        longitude,
        location_name,
        
        -- Ép chuẩn Timezone để đồng bộ với execution_date (tránh lỗi ngầm khi Join)
        CAST(t.time_str AS TIMESTAMP) AT TIME ZONE 'Asia/Bangkok' AS forecast_time,
        
        -- Dùng idx để "móc" dữ liệu ở các mảng khác tương ứng cùng vị trí
        -- Lưu ý: Index của PostgreSQL jsonb array bắt đầu từ 0, nên phải lấy idx - 1
        CAST(raw_json->'hourly'->'pm10'->>(t.idx::int - 1) AS NUMERIC)                  AS pm10,
        CAST(raw_json->'hourly'->'pm2_5'->>(t.idx::int - 1) AS NUMERIC)                 AS pm2_5,
        CAST(raw_json->'hourly'->'carbon_monoxide'->>(t.idx::int - 1) AS NUMERIC)       AS carbon_monoxide,
        CAST(raw_json->'hourly'->'nitrogen_dioxide'->>(t.idx::int - 1) AS NUMERIC)      AS nitrogen_dioxide,
        CAST(raw_json->'hourly'->'sulphur_dioxide'->>(t.idx::int - 1) AS NUMERIC)       AS sulphur_dioxide,
        CAST(raw_json->'hourly'->'ozone'->>(t.idx::int - 1) AS NUMERIC)                 AS ozone,
        CAST(raw_json->'hourly'->'aerosol_optical_depth'->>(t.idx::int - 1) AS NUMERIC) AS aerosol_optical_depth,
        CAST(raw_json->'hourly'->'dust'->>(t.idx::int - 1) AS NUMERIC)                  AS dust,
        CAST(raw_json->'hourly'->'uv_index'->>(t.idx::int - 1) AS NUMERIC)              AS uv_index
        
    FROM source_data,
    LATERAL jsonb_array_elements_text(raw_json->'hourly'->'time') WITH ORDINALITY AS t(time_str, idx)
)

SELECT * FROM extracted_arrays

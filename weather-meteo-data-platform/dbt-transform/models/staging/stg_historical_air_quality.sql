-- CRIT-2 FIX: Liệt kê rõ TỪNG CỘT với đúng thứ tự khớp stg_air_quality_hourly.sql
-- UNION ALL khớp cột theo VỊ TRÍ, không theo tên.
-- Thứ tự phải khớp: raw_id, execution_date, latitude, longitude, location_name,
-- forecast_time, pm10, pm2_5, carbon_monoxide, nitrogen_dioxide,
-- sulphur_dioxide, ozone, aerosol_optical_depth, dust, uv_index

WITH source_data AS (
    SELECT
        id AS raw_id,
        -- LOGIC-3 NOTE: xem stg_historical_weather.sql — cùng lý do dùng ingested_at.
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
        pm10_cams                                 AS pm10,
        pm2_5_cams                                AS pm2_5,
        carbon_monoxide_cams                      AS carbon_monoxide,
        nitrogen_dioxide_cams                     AS nitrogen_dioxide,
        sulphur_dioxide_cams                      AS sulphur_dioxide,
        ozone_cams                                AS ozone,
        CAST(NULL AS NUMERIC)                     AS aerosol_optical_depth,
        CAST(NULL AS NUMERIC)                     AS dust,
        CAST(NULL AS NUMERIC)                     AS uv_index
    FROM {{ source('meteo_bronze', 'bronze_historical_weather') }}
)

-- Thứ tự cột này phải KHỚP CHÍNH XÁC với stg_air_quality_hourly.sql
SELECT
    raw_id,
    execution_date,
    latitude,
    longitude,
    location_name,
    forecast_time,
    pm10,
    pm2_5,
    carbon_monoxide,
    nitrogen_dioxide,
    sulphur_dioxide,
    ozone,
    aerosol_optical_depth,
    dust,
    uv_index
FROM source_data

-- Liệt kê rõ TỪNG CỘT với đúng thứ tự khớp stg_air_quality_hourly.sql
-- UNION ALL khớp cột theo VỊ TRÍ, không theo tên.
-- Thứ tự phải khớp: raw_id, execution_date, latitude, longitude, location_name,
-- forecast_time, pm10, pm2_5, carbon_monoxide, nitrogen_dioxide,
-- sulphur_dioxide, ozone, aerosol_optical_depth, dust, uv_index

WITH source_data AS (
    SELECT
        id AS raw_id,
        -- xem stg_historical_weather.sql — cùng lý do dùng ingested_at.
        ingested_at                               AS execution_date,
        ROUND(CAST(lat AS NUMERIC), 4)            AS latitude,
        ROUND(CAST(lon AS NUMERIC), 4)            AS longitude,
        COALESCE(location_name, 'Unknown Station ' || CAST(location_id AS VARCHAR)) AS location_name,
        datetime                                  AS forecast_time,
        pm10_cams                                 AS pm10,
        pm2_5_cams                                AS pm2_5,
        carbon_monoxide_cams                      AS carbon_monoxide,
        nitrogen_dioxide_cams                     AS nitrogen_dioxide,
        sulphur_dioxide_cams                      AS sulphur_dioxide,
        ozone_cams                                AS ozone,
        aerosol_optical_depth_cams                AS aerosol_optical_depth,
        dust_cams                                 AS dust,
        aq_uv_index_cams                          AS uv_index
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

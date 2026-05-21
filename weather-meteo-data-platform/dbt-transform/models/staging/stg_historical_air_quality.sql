WITH source_data AS (
    SELECT 
        id AS raw_id,
        ingested_at AS execution_date,
        ROUND(CAST(lat AS NUMERIC), 4) AS latitude,
        ROUND(CAST(lon AS NUMERIC), 4) AS longitude,
        datetime AS forecast_time,
        pm10_cams AS pm10,
        pm2_5_cams AS pm2_5,
        carbon_monoxide_cams AS carbon_monoxide,
        nitrogen_dioxide_cams AS nitrogen_dioxide,
        sulphur_dioxide_cams AS sulphur_dioxide,
        ozone_cams AS ozone
    FROM {{ source('meteo_bronze', 'bronze_historical_weather') }}
)

SELECT 
    raw_id,
    execution_date,
    latitude,
    longitude,
    forecast_time,
    pm10,
    pm2_5,
    carbon_monoxide,
    nitrogen_dioxide,
    sulphur_dioxide,
    ozone,
    CAST(NULL AS NUMERIC) AS aerosol_optical_depth,
    CAST(NULL AS NUMERIC) AS dust,
    CAST(NULL AS NUMERIC) AS uv_index
FROM source_data

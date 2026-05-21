WITH source_data AS (
    SELECT 
        id AS raw_id,
        ingested_at AS execution_date,
        ROUND(CAST(lat AS NUMERIC), 4) AS latitude,
        ROUND(CAST(lon AS NUMERIC), 4) AS longitude,
        datetime AS forecast_time,
        temperature_2m,
        relative_humidity_2m,
        precipitation,
        pressure_msl,
        wind_speed_10m,
        wind_direction_10m
    FROM {{ source('meteo_bronze', 'bronze_historical_weather') }}
)

SELECT 
    raw_id,
    execution_date,
    latitude,
    longitude,
    forecast_time,
    temperature_2m,
    relative_humidity_2m,
    CAST(NULL AS NUMERIC) AS dew_point_2m,
    CAST(NULL AS NUMERIC) AS apparent_temperature,
    CAST(NULL AS NUMERIC) AS precipitation_probability,
    precipitation,
    pressure_msl,
    CAST(NULL AS NUMERIC) AS surface_pressure,
    CAST(NULL AS NUMERIC) AS cloud_cover,
    CAST(NULL AS NUMERIC) AS visibility,
    wind_speed_10m,
    wind_direction_10m,
    CAST(NULL AS NUMERIC) AS wind_gusts_10m,
    CAST(NULL AS NUMERIC) AS uv_index
FROM source_data

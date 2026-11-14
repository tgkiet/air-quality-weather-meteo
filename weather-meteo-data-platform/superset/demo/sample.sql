-- Synthetic fixtures for a disposable demo database.
BEGIN;
CREATE SCHEMA IF NOT EXISTS gold_layer;
CREATE TABLE gold_layer.mart_hourly_conditions AS
WITH points(location_name, latitude, longitude, shift) AS (
    VALUES ('HN Quận Hoàn Kiếm', 21.0285, 105.8542, 1),
           ('HCM Quận 1', 10.7758, 106.7009, 3)
), hours AS (
    SELECT reference_hour, hour_offset,
           reference_hour + hour_offset * interval '1 hour' AS forecast_time
    FROM (SELECT date_trunc('hour', now()) AS reference_hour) AS clock
    CROSS JOIN generate_series(-23, 76) AS offsets(hour_offset)
), forecast AS (
    SELECT location_name, latitude, longitude, reference_hour, forecast_time,
           (26 + shift + 4 * sin(hour_offset / 4.0))::numeric(5,1) AS temperature,
           CASE WHEN mod(hour_offset + shift, 11) = 0 THEN 3.5 ELSE 0.0 END AS rain,
           CASE WHEN mod(hour_offset + shift, 11) = 0 THEN 80 ELSE 15 END AS rain_probability,
           CASE WHEN hour_offset BETWEEN 42 AND 45 THEN NULL
                ELSE (28 + shift * 6 + 18 * sin(hour_offset / 6.0))::numeric(6,1) END AS pm25,
           greatest(0, 8 - abs(12 - extract(hour FROM forecast_time AT TIME ZONE 'Asia/Ho_Chi_Minh'))) AS uv
    FROM points CROSS JOIN hours
)
SELECT location_name, latitude, longitude, forecast_time,
       forecast_time AT TIME ZONE 'Asia/Ho_Chi_Minh' AS forecast_time_local,
       reference_hour AS execution_date,
       reference_hour AS weather_ingested_at,
       CASE WHEN pm25 IS NOT NULL THEN reference_hour END AS air_quality_ingested_at,
       temperature AS temperature_2m, temperature + 2 AS apparent_temperature,
       65::numeric AS relative_humidity_2m, 19::numeric AS dew_point_2m,
       rain AS precipitation, rain_probability AS precipitation_probability,
       1010::numeric AS pressure_msl, 1005::numeric AS surface_pressure, 33::numeric AS cloud_cover,
       10000::numeric AS visibility, 10::numeric AS wind_speed_10m,
       190::numeric AS wind_direction_10m, 16::numeric AS wind_gusts_10m, uv AS weather_uv_index,
       pm25 AS pm2_5, pm25 * 1.5 AS pm10,
       200::numeric AS carbon_monoxide, 10::numeric AS nitrogen_dioxide, 4::numeric AS sulphur_dioxide,
       30::numeric AS ozone, 0.2::numeric AS aerosol_optical_depth, 10::numeric AS dust, uv AS aq_uv_index,
       CASE WHEN temperature >= 30 THEN 'Nóng' ELSE 'Dễ chịu' END AS temperature_level,
       CASE WHEN uv >= 8 THEN 'Rất cao' WHEN uv >= 6 THEN 'Cao' WHEN uv >= 3 THEN 'Trung bình' ELSE 'Thấp' END AS uv_level,
       CASE WHEN pm25 IS NULL THEN 'Chưa có dữ liệu' WHEN pm25 >= 55 THEN 'Rất không lành mạnh'
            WHEN pm25 >= 35 THEN 'Không lành mạnh' ELSE 'Trung bình' END AS pm2_5_level,
       uv >= 8 AS is_weather_alert, pm25 >= 55 AS is_air_quality_alert
FROM forecast;
GRANT USAGE ON SCHEMA gold_layer TO meteo_demo;
GRANT SELECT ON gold_layer.mart_hourly_conditions TO meteo_demo;
COMMIT;

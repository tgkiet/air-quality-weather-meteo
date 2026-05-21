{{ config(
    materialized='incremental',
    unique_key=['forecast_time', 'latitude', 'longitude']
) }}

WITH stg_aq AS (
    SELECT * 
    FROM {{ ref('stg_air_quality_hourly') }}
    UNION ALL
    SELECT *
    FROM {{ ref('stg_historical_air_quality') }}
)

-- Sử dụng tuyệt kỹ DISTINCT ON của PostgreSQL để lấy bản cập nhật mới nhất cho mỗi khung giờ,
-- tránh làm Crash lệnh MERGE của dbt khi có nhiều execution_date trả về cùng một forecast_time.
SELECT DISTINCT ON (forecast_time, latitude, longitude)
    *
FROM stg_aq

{% if is_incremental() %}
    WHERE execution_date >= (SELECT COALESCE(max(execution_date), '1900-01-01'::timestamptz) FROM {{ this }})
{% endif %}

ORDER BY forecast_time, latitude, longitude, execution_date DESC

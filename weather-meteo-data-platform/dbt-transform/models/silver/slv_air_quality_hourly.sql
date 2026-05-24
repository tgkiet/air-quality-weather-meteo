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

-- Dùng DISTINCT ON của PostgreSQL để lấy bản cập nhật mới nhất cho mỗi khung giờ.
-- Việc này ngăn lỗi crash lệnh MERGE của dbt do trùng lặp khóa chính.
SELECT DISTINCT ON (forecast_time, latitude, longitude)
    *
FROM stg_aq

{% if is_incremental() %}
    -- LOGIC-3 FIX: Incremental Pattern tối ưu bằng Airflow Context.
    -- Xem chi tiết tại slv_weather_hourly.sql. Pattern này giúp tiết kiệm 100% tài nguyên 
    -- xử lý dư thừa mà vẫn đảm bảo tính Lũy đẳng (Idempotency) khi vận hành Airflow.
    {% if var('execution_date', none) %}
        WHERE execution_date = '{{ var("execution_date") }}'::timestamptz
    {% else %}
        WHERE execution_date >= (SELECT COALESCE(max(execution_date), '1900-01-01'::timestamptz) FROM {{ this }})
    {% endif %}
{% endif %}

ORDER BY forecast_time, latitude, longitude, execution_date DESC

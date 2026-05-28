{{ config(
    materialized='incremental',
    unique_key=['forecast_time', 'latitude', 'longitude']
) }}

WITH stg_weather AS (
    SELECT * 
    FROM {{ ref('stg_weather_hourly') }}
    UNION ALL
    SELECT *
    FROM {{ ref('stg_historical_weather') }}
)

-- Xử lý lặp dữ liệu dự báo.
-- Mỗi lần gọi API trả về dự báo 168h. Hai lần gọi liên tiếp sẽ có các khung giờ trùng nhau.
-- Nếu không lọc bỏ, lệnh MERGE của dbt sẽ lỗi do nhiều dòng nguồn cập nhật cùng 1 dòng đích.
--
-- Giải pháp: Dùng DISTINCT ON của PostgreSQL để lọc trùng ở staging trước khi MERGE.
SELECT DISTINCT ON (forecast_time, latitude, longitude)
    *
FROM stg_weather

{% if is_incremental() %}
    -- Incremental Pattern tối ưu bằng Airflow Context.
    -- Bằng cách nhận tham số `execution_date` từ Airflow, dbt CHỈ xử lý đúng batch của lần chạy đó.
    -- Đảm bảo hiệu suất 100% (không reprocess batch cũ) và an toàn tuyệt đối khi Clear Task / Backfill.
    -- Nếu chạy tay không truyền tham số (vd: debug local), fallback về `>=` để giữ an toàn (dù sẽ quét dư).
    {% if var('execution_date', none) %}
        WHERE execution_date = '{{ var("execution_date") }}'::timestamptz
    {% else %}
        WHERE execution_date >= (SELECT COALESCE(max(execution_date), '1900-01-01'::timestamptz) FROM {{ this }})
    {% endif %}
{% endif %}

-- Bắt buộc ORDER BY bộ khóa trước, sau đó ưu tiên execution_date DESC.
-- Đảm bảo khi có trùng forecast_time, dòng được gọi API gần nhất (dự báo mới nhất) sẽ được giữ lại.
ORDER BY forecast_time, latitude, longitude, execution_date DESC

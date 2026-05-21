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

-- LỖI LOGIC ĐÃ ĐƯỢC FIX: 
-- API Open-Meteo trả về dự báo của 24h tới. Nếu Airflow chạy lúc 10h và 11h, 
-- cả 2 mẻ dữ liệu này đều chứa dự báo cho lúc 12h, 13h... (Bị lặp forecast_time).
-- Nếu không loại bỏ các dòng trùng lặp này NGAY TẠI ĐÂY, lệnh MERGE của dbt sẽ bị Crash
-- vì vi phạm nguyên tắc "Một dòng đích không thể bị update 2 lần trong 1 câu lệnh".

-- Sử dụng tuyệt kỹ DISTINCT ON của riêng PostgreSQL để giải quyết triệt để:
SELECT DISTINCT ON (forecast_time, latitude, longitude)
    *
FROM stg_weather

{% if is_incremental() %}
    -- Lọc lấy dữ liệu từ mẻ chạy cũ nhất đến mẻ mới nhất.
    -- Ép kiểu rõ ràng '1900-01-01'::timestamp để tránh lỗi Type Casting của Postgres
    WHERE execution_date >= (SELECT COALESCE(max(execution_date), '1900-01-01'::timestamptz) FROM {{ this }})
{% endif %}

-- Bắt buộc phải ORDER BY theo bộ khóa, sau đó ưu tiên execution_date DESC
-- Nghĩa là nếu có nhiều dòng trùng forecast_time, nó sẽ lấy dòng được gọi API gần nhất (Dự báo mới nhất và chính xác nhất)
ORDER BY forecast_time, latitude, longitude, execution_date DESC

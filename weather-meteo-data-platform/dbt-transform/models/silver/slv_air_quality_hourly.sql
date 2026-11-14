{{ config(unique_key=['forecast_time', 'latitude', 'longitude']) }}

{{ latest_forecast_rows('stg_air_quality_hourly', 'stg_historical_air_quality') }}

{{ config(unique_key=['forecast_time', 'latitude', 'longitude']) }}

{{ latest_forecast_rows('stg_weather_hourly', 'stg_historical_weather') }}

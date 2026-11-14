-- The backfill and dbt staging models share this Bronze schema.
ALTER TABLE public.bronze_historical_weather
    ADD COLUMN IF NOT EXISTS dew_point_2m NUMERIC,
    ADD COLUMN IF NOT EXISTS apparent_temperature NUMERIC,
    ADD COLUMN IF NOT EXISTS surface_pressure NUMERIC,
    ADD COLUMN IF NOT EXISTS cloud_cover NUMERIC,
    ADD COLUMN IF NOT EXISTS wind_gusts_10m NUMERIC,
    ADD COLUMN IF NOT EXISTS uv_index NUMERIC,
    ADD COLUMN IF NOT EXISTS aerosol_optical_depth_cams NUMERIC,
    ADD COLUMN IF NOT EXISTS dust_cams NUMERIC,
    ADD COLUMN IF NOT EXISTS aq_uv_index_cams NUMERIC;

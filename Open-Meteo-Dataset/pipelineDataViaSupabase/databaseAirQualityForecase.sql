-- Bước 1: Xoá bảng cũ nếu tồn tại
DROP TABLE IF EXISTS public.air_quality_forecast_data;

-- Bước 2: Tạo bảng mới với cấu trúc
CREATE TABLE public.air_quality_forecast_data (
    -- Khoá định danh địa điểm
    location_id BIGINT NOT NULL,

    -- Dấu thời gian có timezone
    datetime TIMESTAMPTZ NOT NULL,

    -- Dữ liệu khí tượng
    temperature_2m REAL,
    relative_humidity_2m REAL,
    precipitation REAL,
    rain REAL,
    wind_speed_10m REAL,
    wind_direction_10m REAL,
    pressure_msl REAL,
    boundary_layer_height REAL,

    -- Dữ liệu chất lượng không khí
    pm10_cams REAL,
    pm2_5_cams REAL,
    carbon_monoxide_cams REAL,
    nitrogen_dioxide_cams REAL,
    sulphur_dioxide_cams REAL,
    ozone_cams REAL,

    -- Toạ độ (mới thêm)
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,

    -- Khoá chính đảm bảo duy nhất cho từng thời điểm & địa điểm
    CONSTRAINT air_quality_forecast_data_pkey PRIMARY KEY (location_id, datetime)
);

COMMENT ON TABLE public.air_quality_forecast_data IS 
'Bảng tổng hợp dữ liệu khí tượng + chất lượng không khí (Open-Meteo) cho các địa điểm tại Hà Nội, bao gồm toạ độ lat/lon.';

#!/bin/bash
set -e

# File script này sẽ được chạy tự động bởi PostgreSQL container khi khởi tạo.
# Lợi ích của file .sh so với file .sql là chúng ta có thể truyền biến môi trường từ .env vào đây!

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    -- =========================================================================
    -- PHẦN 1: CÀI ĐẶT CHO AIRFLOW DATABASE
    -- =========================================================================
    -- Tạo User riêng cho Airflow
    CREATE USER $AIRFLOW_DB_USER WITH PASSWORD '$AIRFLOW_DB_PASSWORD';

    -- Tạo Database riêng cho Airflow (tách biệt hoàn toàn với Data Warehouse)
    CREATE DATABASE $AIRFLOW_DB_NAME;

    -- Cấp toàn quyền cho Airflow User trên Database này
    GRANT ALL PRIVILEGES ON DATABASE $AIRFLOW_DB_NAME TO $AIRFLOW_DB_USER;

    -- Đối với Postgres 15+, cần cấp thêm quyền trên schema public
    \c $AIRFLOW_DB_NAME
    GRANT ALL ON SCHEMA public TO $AIRFLOW_DB_USER;

EOSQL

# =========================================================================
# PHẦN 2: CÀI ĐẶT CHO DATA WAREHOUSE (air_quality_db)
# =========================================================================
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    -- Tạo bảng Bronze Layer để lưu trữ dữ liệu thô từ Open-Meteo API
    CREATE TABLE IF NOT EXISTS api_openmeteo_raw_data (
        id              SERIAL PRIMARY KEY,
        source_type     VARCHAR(100)    NOT NULL,   -- Phân biệt nguồn: 'weather_forecast_hourly' | 'air_quality_hourly'
        execution_date  TIMESTAMPTZ     NOT NULL,   -- Mốc thời gian Airflow đã lập lịch (Logical Date), KHÔNG phải thời gian Insert thực tế
        raw_json        JSONB           NOT NULL,   -- Toàn bộ cục JSON trả về từ API, lưu nguyên vẹn ở Bronze Layer
        ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()  -- Thời điểm thực tế dòng dữ liệu này được ghi vào DB
    );

    -- -------------------------------------------------------------------------
    -- "MỎ NEO" CHO TÍNH LŨY ĐẲNG (IDEMPOTENCY ANCHOR):
    -- Unique Constraint này là điều kiện TIÊN QUYẾT cho câu lệnh UPSERT
    -- (INSERT ... ON CONFLICT DO UPDATE) trong postgres_loader.py hoạt động.
    --
    -- Ý nghĩa: Với cùng một nguồn dữ liệu (source_type) và cùng một mốc
    -- thời gian Airflow (execution_date), Database KHÔNG BAO GIỜ được phép
    -- tồn tại 2 dòng. Đây là ràng buộc vật lý ở tầng cơ sở dữ liệu.
    -- -------------------------------------------------------------------------
    ALTER TABLE api_openmeteo_raw_data
    ADD CONSTRAINT unique_source_execution_date
    UNIQUE (source_type, execution_date);

EOSQL

# BUG-6 FIX: Tạo bảng bronze_historical_weather (cho CSV + HCM backfill)
# trong một psql session riêng để tránh transaction nesting issue.
# Cột location_name được thêm vào để phân biệt tên địa điểm (HCM Quận 1, Hanoi Station...).
# DO $$ LANGUAGE plpgsql để xử lý migration-safe (không lỗi nếu cột đã tồn tại).
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    CREATE TABLE IF NOT EXISTS bronze_historical_weather (
        id                      SERIAL PRIMARY KEY,
        datetime                TIMESTAMPTZ NOT NULL,
        temperature_2m          NUMERIC,
        relative_humidity_2m    NUMERIC,
        precipitation           NUMERIC,
        rain                    NUMERIC,
        wind_speed_10m          NUMERIC,
        wind_direction_10m      NUMERIC,
        pressure_msl            NUMERIC,
        boundary_layer_height   NUMERIC,
        pm10_cams               NUMERIC,
        pm2_5_cams              NUMERIC,
        carbon_monoxide_cams    NUMERIC,
        nitrogen_dioxide_cams   NUMERIC,
        sulphur_dioxide_cams    NUMERIC,
        ozone_cams              NUMERIC,
        location_id             NUMERIC,
        lat                     NUMERIC,
        lon                     NUMERIC,
        location_name           VARCHAR(255),   -- Tên địa điểm: "HCM Quận 1", "HN Đống Đa", ...
        ingested_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    -- -------------------------------------------------------------------------
    -- "MỎ NEO" CHO TÍNH LŨY ĐẲNG CỦA BRONZE HISTORICAL:
    -- BUG-NEW-1 FIX: Thêm UNIQUE constraint vào init_dbs.sh thay vì chỉ trong
    -- csv_loader.py. Không có constraint này, backfill_history.py sẽ FAIL
    -- với "there is no unique or exclusion constraint matching the ON CONFLICT
    -- specification" nếu backfill chạy TRƯỚC csv_loader.
    -- Constraint này cũng đảm bảo csv_loader UPSERT hoạt động đúng.
    -- -------------------------------------------------------------------------
    ALTER TABLE bronze_historical_weather
    ADD CONSTRAINT unique_historical_datetime_lat_lon
    UNIQUE (datetime, lat, lon);

    -- Migration-safe: Thêm cột location_name nếu chưa tồn tại
    -- Dùng \$BODY\$ thay vì \$\$ vì \$\$ bị bash expand thành PID trong heredoc
    DO \$BODY\$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name   = 'bronze_historical_weather'
              AND column_name  = 'location_name'
        ) THEN
            ALTER TABLE bronze_historical_weather ADD COLUMN location_name VARCHAR(255);
        END IF;
    END \$BODY\$;

EOSQL

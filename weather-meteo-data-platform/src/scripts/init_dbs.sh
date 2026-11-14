#!/bin/bash
set -e

# File script này sẽ được chạy tự động bởi PostgreSQL container khi khởi tạo.
# Lợi ích của file .sh so với file .sql là chúng ta có thể truyền biến môi trường từ .env vào đây!

create_app_database() {
    # Read credentials from the environment without expanding them into SQL or command arguments.
    APP_DB_USER="$1" APP_DB_PASSWORD="$2" APP_DB_NAME="$3" \
    psql -X -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'
	\getenv app_user APP_DB_USER
	\getenv app_password APP_DB_PASSWORD
	\getenv app_database APP_DB_NAME
	CREATE USER :"app_user" WITH PASSWORD :'app_password';
	CREATE DATABASE :"app_database";
	GRANT ALL PRIVILEGES ON DATABASE :"app_database" TO :"app_user";
	EOSQL

    APP_DB_USER="$1" psql -X -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$3" <<-'EOSQL'
	\getenv app_user APP_DB_USER
	GRANT ALL ON SCHEMA public TO :"app_user";
	EOSQL
}

create_app_database "$AIRFLOW_DB_USER" "$AIRFLOW_DB_PASSWORD" "$AIRFLOW_DB_NAME"

# =========================================================================
# PHẦN 2: CÀI ĐẶT CHO DATA WAREHOUSE (air_quality_db)
# =========================================================================
psql -X -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'

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

# Tạo bảng bronze_historical_weather (cho Archive API backfill toàn bộ 52 khu vực)
# trong một psql session riêng để tránh transaction nesting issue.
# Cột location_name được thêm vào để phân biệt tên địa điểm (HCM Quận 1, Hanoi Station...).
# DO $$ LANGUAGE plpgsql để xử lý migration-safe (không lỗi nếu cột đã tồn tại).
psql -X -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'

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
    -- "MO NEO" CHO TINH LUY DANG CUA BRONZE HISTORICAL:
    -- Constraint nay la tieu kien de UPSERT (ON CONFLICT DO UPDATE)
    -- trong backfill_history.py hoat dong dung.
    -- Khong co constraint nay → UPSERT se fail ngay lap tuc.
    -- -------------------------------------------------------------------------
    ALTER TABLE bronze_historical_weather
    ADD CONSTRAINT unique_historical_datetime_lat_lon
    UNIQUE (datetime, lat, lon);

    -- Migration-safe: Thêm cột location_name nếu chưa tồn tại
    DO $BODY$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name   = 'bronze_historical_weather'
              AND column_name  = 'location_name'
        ) THEN
            ALTER TABLE bronze_historical_weather ADD COLUMN location_name VARCHAR(255);
        END IF;
    END $BODY$;

    -- =========================================================================
    -- TẠO BẢNG ALERT HISTORY (Dùng cho alert_job.py chống spam)
    -- =========================================================================
    CREATE SCHEMA IF NOT EXISTS silver_layer;
    
    CREATE TABLE IF NOT EXISTS silver_layer.alert_history (
        id SERIAL PRIMARY KEY,
        location_name VARCHAR(255) NOT NULL,
        forecast_time TIMESTAMPTZ NOT NULL,
        alert_type VARCHAR(50) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT unique_alert_history UNIQUE(location_name, forecast_time, alert_type)
    );

EOSQL

# =========================================================================
# PHẦN 3: CÀI ĐẶT CHO SUPERSET DATABASE (Metadata: Dashboard, Chart, User)
# =========================================================================
# Superset cần 1 database riêng để lưu cấu hình nội bộ (không phải data của bạn).
# Tách riêng để dễ backup, restore, và không ảnh hưởng đến air_quality_db.
create_app_database "$SUPERSET_DB_USER" "$SUPERSET_DB_PASSWORD" "$SUPERSET_DB_NAME"

# =========================================================================
# PHẦN 4: CẤP QUYỀN ĐỌC GOLD LAYER CHO SUPERSET
# =========================================================================
# Superset chỉ cần quyền SELECT (đọc) vào air_quality_db.gold_layer.
# KHÔNG cấp quyền ghi — nguyên tắc Least Privilege (đặc quyền tối thiểu).
#
# Phải PRE-CREATE schema gold_layer trước khi GRANT.
# Lý do: init_dbs.sh chạy khi PostgreSQL khởi tạo lần đầu, trước khi dbt chạy.
# Nếu GRANT USAGE ON SCHEMA gold_layer mà schema chưa tồn tại → lỗi "schema does not exist".
# Giải pháp: tạo schema tại đây, dbt sẽ dùng schema đã có để tạo bảng (dbt không xóa schema).
psql -X -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'

\getenv warehouse_database POSTGRES_DB
\getenv warehouse_owner POSTGRES_USER
\getenv superset_user SUPERSET_DB_USER

    -- Cấp quyền kết nối vào warehouse cho Superset.
    GRANT CONNECT ON DATABASE :"warehouse_database" TO :"superset_user";

    CREATE SCHEMA IF NOT EXISTS gold_layer;
    GRANT USAGE ON SCHEMA gold_layer TO :"superset_user";
    GRANT SELECT ON ALL TABLES IN SCHEMA gold_layer TO :"superset_user";

    -- Áp dụng cho bảng được tạo sau này bởi tài khoản dbt.
    ALTER DEFAULT PRIVILEGES
        FOR USER :"warehouse_owner"
        IN SCHEMA gold_layer
        GRANT SELECT ON TABLES TO :"superset_user";

EOSQL

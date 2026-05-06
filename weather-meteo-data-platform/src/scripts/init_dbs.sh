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

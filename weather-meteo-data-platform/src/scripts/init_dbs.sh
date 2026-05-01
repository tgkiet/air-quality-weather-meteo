#!/bin/bash
set -e

# File script này sẽ được chạy tự động bởi PostgreSQL container khi khởi tạo.
# Lợi ích của file .sh so với file .sql là chúng ta có thể truyền biến môi trường từ .env vào đây!

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Tạo User riêng cho Airflow
    CREATE USER $AIRFLOW_DB_USER WITH PASSWORD '$AIRFLOW_DB_PASSWORD';
    
    -- Tạo Database riêng cho Airflow
    CREATE DATABASE $AIRFLOW_DB_NAME;
    
    -- Cấp toàn quyền cho Airflow User trên Database này
    GRANT ALL PRIVILEGES ON DATABASE $AIRFLOW_DB_NAME TO $AIRFLOW_DB_USER;
    
    -- Đối với Postgres 15+, cần cấp thêm quyền trên schema public
    \c $AIRFLOW_DB_NAME
    GRANT ALL ON SCHEMA public TO $AIRFLOW_DB_USER;
EOSQL

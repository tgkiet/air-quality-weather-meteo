import os
import csv
from psycopg2 import sql, extras, Error
from src.loaders.base_loader import BasePostgresLoader
from src.utils.logger import get_logger

logger = get_logger("CSVLoader")

class CSVLoader(BasePostgresLoader):
    def __init__(self):
        super().__init__()

    def create_table_if_not_exists(self):
        """
        Tạo bảng bronze_historical_weather nếu chưa tồn tại, kèm theo UNIQUE CONSTRAINT
        trên (datetime, lat, lon) để đảm bảo tính lũy đẳng (Idempotency).
        """
        if not self.connection or self.connection.closed:
            raise ConnectionError("Database connection is not established or has been closed.")

        create_table_query = """
        CREATE TABLE IF NOT EXISTS bronze_historical_weather (
            id SERIAL PRIMARY KEY,
            datetime TIMESTAMPTZ NOT NULL,
            temperature_2m NUMERIC,
            relative_humidity_2m NUMERIC,
            precipitation NUMERIC,
            rain NUMERIC,
            wind_speed_10m NUMERIC,
            wind_direction_10m NUMERIC,
            pressure_msl NUMERIC,
            boundary_layer_height NUMERIC,
            pm10_cams NUMERIC,
            pm2_5_cams NUMERIC,
            carbon_monoxide_cams NUMERIC,
            nitrogen_dioxide_cams NUMERIC,
            sulphur_dioxide_cams NUMERIC,
            ozone_cams NUMERIC,
            location_id NUMERIC,
            lat NUMERIC,
            lon NUMERIC,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        
        add_constraint_query = """
        ALTER TABLE bronze_historical_weather 
        ADD CONSTRAINT unique_historical_datetime_lat_lon 
        UNIQUE (datetime, lat, lon);
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_query)
                
                # Kiểm tra xem constraint đã tồn tại chưa trước khi thêm
                cursor.execute("""
                    SELECT constraint_name 
                    FROM information_schema.table_constraints 
                    WHERE table_name = 'bronze_historical_weather' 
                      AND constraint_name = 'unique_historical_datetime_lat_lon';
                """)
                if not cursor.fetchone():
                    logger.info("Adding UNIQUE constraint (datetime, lat, lon) to bronze_historical_weather...")
                    cursor.execute(add_constraint_query)
                
            self.connection.commit()
            logger.info("Table bronze_historical_weather is ready.")
        except Error as e:
            self.connection.rollback()
            logger.error(f"Error creating/configuring bronze_historical_weather table: {e}")
            raise e

    def load_csv(self, csv_file_path: str):
        """
        Nạp file CSV lịch sử vào PostgreSQL sử dụng kỹ thuật COPY + UPSERT qua TEMP TABLE.
        Đảm bảo hiệu suất vượt trội cho tệp dữ liệu lớn (>800k dòng) và giữ tính lũy đẳng.
        """
        if not self.connection or self.connection.closed:
            raise ConnectionError("Database connection is not established or has been closed.")

        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"CSV file not found at: {csv_file_path}")

        logger.info(f"Starting to load CSV: {csv_file_path}")
        
        # 1. Đọc header của CSV để xác định danh sách cột và thứ tự tương ứng
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            header_line = f.readline().strip()
            # Bỏ ký tự BOM nếu có và split
            if header_line.startswith('\ufeff'):
                header_line = header_line[1:]
            csv_columns = [col.strip() for col in header_line.split(',')]

        logger.info(f"Detected columns from CSV header: {csv_columns}")

        try:
            with self.connection.cursor() as cursor:
                # 2. Tạo bảng tạm (TEMP TABLE) khớp với cấu trúc bảng chính nhưng không có constraints để tối ưu tốc độ COPY
                # ON COMMIT DROP để Postgres tự động dọn dẹp khi kết thúc transaction
                cursor.execute("""
                    CREATE TEMP TABLE temp_historical_weather (
                        LIKE bronze_historical_weather INCLUDING DEFAULTS
                    ) ON COMMIT DROP;
                """)
                
                # Loại bỏ cột 'id' và 'ingested_at' khỏi việc COPY vì chúng được sinh tự động
                # Ta chỉ COPY đúng các cột có trong CSV
                columns_str = ", ".join([f'"{col}"' for col in csv_columns])
                
                # 3. Sử dụng COPY EXPERT để stream dữ liệu nhanh chóng từ Client lên Server
                # Kỹ thuật này nhanh gấp 10-20 lần so với INSERT thông thường và không tốn bộ nhớ RAM của Client
                copy_query = f"COPY temp_historical_weather ({columns_str}) FROM STDIN WITH CSV HEADER"
                
                logger.info(f"Streaming data to temporary table using: {copy_query}")
                with open(csv_file_path, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(copy_query, f)
                
                # Lấy số dòng đã load vào temp table
                cursor.execute("SELECT count(*) FROM temp_historical_weather;")
                temp_count = cursor.fetchone()[0]
                logger.info(f"Successfully copied {temp_count} rows from CSV into temp table.")

                # 4. Thực hiện UPSERT (INSERT ... ON CONFLICT DO UPDATE) từ TEMP TABLE sang BẢNG CHÍNH
                # Cách này đảm bảo Idempotency: nếu chạy lại tệp CSV cũ, dữ liệu chỉ được cập nhật chứ không bị nhân đôi.
                
                # Chuẩn bị danh sách các cột gán cho phần UPDATE
                non_key_cols = [col for col in csv_columns if col not in ('datetime', 'lat', 'lon')]
                update_set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in non_key_cols])
                
                upsert_query = f"""
                    INSERT INTO bronze_historical_weather ({columns_str})
                    SELECT {columns_str}
                    FROM temp_historical_weather
                    ON CONFLICT (datetime, lat, lon)
                    DO UPDATE SET {update_set_clause};
                """
                
                logger.info("Executing set-based UPSERT to merge data into bronze_historical_weather...")
                cursor.execute(upsert_query)
                rows_affected = cursor.rowcount
                logger.info(f"UPSERT complete. Rows affected (inserted or updated): {rows_affected}")

            self.connection.commit()
            logger.info(f"Successfully finished loading CSV: {csv_file_path}")
            return temp_count
            
        except Error as e:
            self.connection.rollback()
            logger.error(f"PostgreSQL Database error loading CSV: {e}")
            raise e
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Unexpected error loading CSV: {e}")
            raise e

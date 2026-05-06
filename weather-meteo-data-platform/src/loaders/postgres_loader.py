import json
import os
import time
import psycopg2
from psycopg2 import sql, extras, Error
from dotenv import load_dotenv
from src.utils.logger import get_logger
from src.utils.config_manager import config_manager

logger = get_logger("PostgresLoader")

class PostgresLoader:
    def __init__(self):
        self.connection = None # Initialize connection attribute
        
        load_dotenv() # Load environment variables from .env file
        
        # Tối ưu 1: Fail-fast Validation cho Environment Variables
        required_vars = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

        self.db_name = os.getenv("POSTGRES_DB")
        self.db_user = os.getenv("POSTGRES_USER")
        self.db_password = os.getenv("POSTGRES_PASSWORD")
        self.db_host = os.getenv("POSTGRES_HOST")
        self.db_port = os.getenv("POSTGRES_PORT")
        
    def connect(self):
        # Tối ưu 2: Tránh rò rỉ (leak) connection nếu hàm connect() bị gọi 2 lần
        if self.connection and not self.connection.closed:
            logger.info("Connection is already open.")
            return self.connection

        db_config = config_manager.database_config
        max_retries = db_config.get("max_retries", 3)
        retry_delay = db_config.get("retry_delay_sec", 5)
        for i in range(max_retries):
            logger.info(f"Connecting to PostgreSQL database (Attempt {i+1}/{max_retries})...")
            try:
                conn = psycopg2.connect(
                    dbname = self.db_name,
                    user = self.db_user,
                    password = self.db_password,
                    host = self.db_host,
                    port = self.db_port
                )
                logger.info("Connection successful!")
                self.connection = conn # Store the connection for later use
                return conn
            # Tối ưu 3: Bắt đúng lỗi Database thay vì Exception rác
            except Error as e:
                logger.warning(f"Database error: {e} (Attempt {i+1} of {max_retries})")
                time.sleep(retry_delay)
        
        logger.error(f"Failed to connect to PostgreSQL database after {max_retries} attempts")
        raise ConnectionError(f"Failed to connect to PostgreSQL database after {max_retries} attempts")
    
    def insert_data(self, table_name: str, source_type: str, execution_date: str, raw_json: dict):
        """
        Insert dữ liệu JSON thô vào PostgreSQL có cơ chế CHỐNG TRÙNG LẶP (Idempotency).
        """
        # Tối ưu 4: Kiểm tra sát sao trạng thái thực của connection thay vì chỉ check None
        if not self.connection or self.connection.closed:
            logger.error("Connection is dead or not established!")
            raise ConnectionError("Database connection is not established or has been closed.")
            
        try:
            with self.connection.cursor() as cursor:
                table_identifier = sql.Identifier(table_name)
                
                # BƯỚC 1: UPSERT DỮ LIỆU (Tối ưu MVCC & Enforce Idempotency)
                # Yêu cầu bảng phải có UNIQUE CONSTRAINT trên (source_type, execution_date)
                upsert_query = sql.SQL("""
                    INSERT INTO {} (source_type, execution_date, raw_json) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT (source_type, execution_date) 
                    DO UPDATE SET raw_json = EXCLUDED.raw_json
                """).format(table_identifier)
                
                # Tối ưu 5: Dùng psycopg2.extras.Json để parse JSONB cực nhanh
                cursor.execute(upsert_query, (source_type, execution_date, extras.Json(raw_json)))
            
            self.connection.commit()
            logger.info(f"Successfully inserted {source_type} data into {table_name}")
            
        except Error as e:
            logger.error(f"PostgreSQL Error inserting {source_type} data into {table_name}: {e}")
            self.connection.rollback()
            raise e
        except Exception as e:
            logger.error(f"Unexpected Error inserting {source_type} data: {e}")
            self.connection.rollback()
            raise e
            
    def close(self):
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("PostgreSQL connection closed.")
import json
import os
import time
import psycopg2
from psycopg2 import sql, extras, Error
from dotenv import load_dotenv
from src.utils.logger import get_logger
from src.utils.config_manager import config_manager
from src.loaders.base_loader import BasePostgresLoader

logger = get_logger("PostgresLoader")

class PostgresLoader(BasePostgresLoader):
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
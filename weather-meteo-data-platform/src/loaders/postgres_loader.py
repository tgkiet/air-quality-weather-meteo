import json
import os
import time
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from src.utils.logger import get_logger

logger = get_logger("PostgresLoader")

class PostgresLoader:
    def __init__(self):
        self.connection = None # Initialize connection attribute
        
        load_dotenv() # Load environment variables from .env file
        self.db_name = os.getenv("POSTGRES_DB")
        self.db_user = os.getenv("POSTGRES_USER")
        self.db_password = os.getenv("POSTGRES_PASSWORD")
        self.db_host = os.getenv("POSTGRES_HOST")
        self.db_port = os.getenv("POSTGRES_PORT")
        
    def connect(self):
        max_retries = 3
        retry_delay = 5  # seconds
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
            except Exception as e:
                logger.warning(f"Error connecting to PostgreSQL database: {e} (Attempt {i+1} of {max_retries})")
                time.sleep(retry_delay)
        
        logger.error(f"Failed to connect to PostgreSQL database after {max_retries} attempts")
        raise Exception(f"Failed to connect to PostgreSQL database after {max_retries} attempts")
    
    def insert_data(self, table_name, source_type, raw_json):
        try:
            cursor = self.connection.cursor()
            # Sử dụng psycopg2.sql.Identifier để CHỐNG SQL Injection hoàn toàn cho table_name
            insert_query = sql.SQL("INSERT INTO {} (source_type, raw_json) VALUES (%s, %s)").format(
                sql.Identifier(table_name)
            )
            json_string = json.dumps(raw_json)
            cursor.execute(insert_query, (source_type, json_string))
            self.connection.commit()
            cursor.close()
            logger.info(f"Successfully inserted {source_type} data into {table_name}")
        except Exception as e:
            logger.error(f"Error inserting {source_type} data into {table_name}: {e}")
            self.connection.rollback()
            
    def close(self):
        if self.connection:
            self.connection.close()
            logger.info("PostgreSQL connection closed.")
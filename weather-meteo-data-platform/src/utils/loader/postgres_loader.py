import json
import os
import time
import psycopg2
from dotenv import load_dotenv

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
            print(f"Connecting to PostgreSQL database (Attempt {i+1}/{max_retries})...")
            try:
                conn = psycopg2.connect(
                    dbname = self.db_name,
                    user = self.db_user,
                    password = self.db_password,
                    host = self.db_host,
                    port = self.db_port
                )
                print("Connection successful!")
                self.connection = conn # Store the connection for later use
                return conn
            except Exception as e:
                print(f"Error connecting to PostgreSQL database: {e} (Attempt {i+1} of {max_retries})")
                time.sleep(retry_delay)
        
        raise Exception(f"Failed to connect to PostgreSQL database after {max_retries} attempts")
    
    def insert_data(self, source_type, raw_json):
        try:
            cursor = self.connection.cursor() # hàm này dùng để tạo một con trỏ (cursor) để thực hiện các lệnh SQL trên DB 
            insert_query = "INSERT INTO api_openmeteo_raw_data (source_type, raw_json) VALUES (%s, %s)"
            json_string = json.dumps(raw_json) # chuyển đổi thành một chuỗi JSON
            cursor.execute(insert_query, (source_type, json_string)) # thực thi câu lệnh SQL với các giá trị được cung cấp
            self.connection.commit() # lưu các thay đổi vào cơ sở dữ liệu
            cursor.close() # đóng cursor sau khi hoàn thành
        except Exception as e:
            print(f"Error inserting data into PostgreSQL database: {e}")
            self.connection.rollback() # nếu có lỗi, hoàn tác các thay đổi đã thực hiện
            
            
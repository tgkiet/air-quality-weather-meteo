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
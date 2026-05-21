import os
import time
import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv
from src.utils.logger import get_logger
from src.utils.config_manager import config_manager

logger = get_logger("BasePostgresLoader")

class BasePostgresLoader:
    def __init__(self):
        self.connection = None
        
        load_dotenv()  # Load environment variables from .env file
        
        # Fail-fast Validation for Environment Variables
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
                self.connection = conn
                return conn
            except Error as e:
                logger.warning(f"Database error: {e} (Attempt {i+1} of {max_retries})")
                time.sleep(retry_delay)
        
        logger.error(f"Failed to connect to PostgreSQL database after {max_retries} attempts")
        raise ConnectionError(f"Failed to connect to PostgreSQL database after {max_retries} attempts")
        
    def close(self):
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("PostgreSQL connection closed.")

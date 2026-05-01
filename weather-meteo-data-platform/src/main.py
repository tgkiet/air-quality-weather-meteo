import os
import sys

# Ensure the root of the project is in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.extractors.open_meteo import OpenMeteoExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.utils.logger import get_logger

logger = get_logger("MainPipeline")

def main():
    logger.info("--- Bắt đầu chạy luồng ELT ở Local ---")
    
    # EXTRACT (Lấy cả Thời tiết & Chất lượng không khí)
    from src.utils.config_manager import ConfigManager
    config = ConfigManager().get_config()
    open_meteo_config = config["api"]["open_meteo"]
    
    # Cấu hình lấy dữ liệu Thời tiết (Weather & Meteo)
    weather_url = open_meteo_config["weather_url"]
    weather_params = open_meteo_config["weather_params"]
    
    # Cấu hình lấy dữ liệu Chất lượng không khí (Air Quality)
    aq_url = open_meteo_config["aq_url"]
    aq_params = open_meteo_config["aq_params"]
    
    logger.info("1. Đang lấy dữ liệu Thời Tiết từ Open-Meteo API...")
    weather_extractor = OpenMeteoExtractor(weather_url)
    try:
        weather_data = weather_extractor.get_open_meteo_data(weather_params)
        logger.info("-> Lấy dữ liệu Thời Tiết thành công!")
    except Exception as e:
        logger.error(f"-> Lỗi khi Extract Thời Tiết: {e}")
        raise

    logger.info("2. Đang lấy dữ liệu Chất lượng không khí từ Open-Meteo API...")
    aq_extractor = OpenMeteoExtractor(aq_url)
    try:
        aq_data = aq_extractor.get_open_meteo_data(aq_params)
        logger.info("-> Lấy dữ liệu Chất Lượng Không Khí thành công!")
    except Exception as e:
        logger.error(f"-> Lỗi khi Extract Chất lượng không khí: {e}")
        raise

    # LOAD (Đẩy cả 2 cục dữ liệu vào DB)
    logger.info("\n3. Đang đẩy dữ liệu vào PostgreSQL...")
    loader = PostgresLoader()
    try:
        loader.connect()
        
        # Đẩy dữ liệu thời tiết
        logger.info("-> Đang Load dữ liệu Thời Tiết...")
        loader.insert_data(
            table_name="api_openmeteo_raw_data", 
            source_type="weather_forecast_hourly", 
            raw_json=weather_data
        )
        
        # Đẩy dữ liệu chất lượng không khí
        logger.info("-> Đang Load dữ liệu Chất lượng không khí...")
        loader.insert_data(
            table_name="api_openmeteo_raw_data", 
            source_type="air_quality_hourly", 
            raw_json=aq_data
        )
    except Exception as e:
        logger.error(f"-> Lỗi khi Load: {e}")
        raise
    finally:
        loader.close()

    logger.info("\n--- Hoàn thành luồng ELT ---")

if __name__ == "__main__":
    main()

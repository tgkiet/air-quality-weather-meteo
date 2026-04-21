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
    
    # Cấu hình lấy dữ liệu Thời tiết (Weather & Meteo)
    weather_url = "https://api.open-meteo.com/v1/forecast"
    weather_params = {
        "latitude": 10.7756, "longitude": 106.7019, # TP.HCM
        "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "hourly": "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation_probability,precipitation,pressure_msl,surface_pressure,cloud_cover,visibility,wind_speed_10m,wind_direction_10m,wind_gusts_10m,uv_index",
        "timezone": "Asia/Bangkok",
    }
    
    # Cấu hình lấy dữ liệu Chất lượng không khí (Air Quality)
    aq_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    aq_params = {
        "latitude": 10.7756, "longitude": 106.7019, # TP.HCM
        "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,aerosol_optical_depth,dust,uv_index",
        "timezone": "Asia/Bangkok",
    }
    
    logger.info("1. Đang lấy dữ liệu Thời Tiết từ Open-Meteo API...")
    weather_extractor = OpenMeteoExtractor(weather_url)
    try:
        weather_data = weather_extractor.get_open_meteo_data(weather_params)
        logger.info("-> Lấy dữ liệu Thời Tiết thành công!")
    except Exception as e:
        logger.error(f"-> Lỗi khi Extract Thời Tiết: {e}")
        return

    logger.info("2. Đang lấy dữ liệu Chất lượng không khí từ Open-Meteo API...")
    aq_extractor = OpenMeteoExtractor(aq_url)
    try:
        aq_data = aq_extractor.get_open_meteo_data(aq_params)
        logger.info("-> Lấy dữ liệu Chất Lượng Không Khí thành công!")
    except Exception as e:
        logger.error(f"-> Lỗi khi Extract Chất lượng không khí: {e}")
        return

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
    finally:
        loader.close()

    logger.info("\n--- Hoàn thành luồng ELT ---")

if __name__ == "__main__":
    main()

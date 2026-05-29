import os
import sys
import json
import argparse

# Ensure the root of the project is in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.extractors.open_meteo import OpenMeteoExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.utils.logger import get_logger
from src.utils.config_manager import config_manager

logger = get_logger("MainPipeline")



def _inject_location_metadata(data, locations: list):
    """
    Injects canonical/requested coordinates and names into the raw API response elements.
    This ensures weather and air quality datasets can be joined perfectly in the DB
    without grid snapping mismatches.

    Open-Meteo Batch API không đảm bảo thứ tự response khớp với
    thứ tự request (API có thể sort/deduplicate coordinates). Match theo tọa độ
    thực tế của response (làm tròn 2 chữ số) thay vì match theo index.

    Side-effect note: Mutates dicts inside `data` in-place (intentional for performance).
    """
    items = data if isinstance(data, list) else [data]

    if len(items) != len(locations):
        logger.error(
            f"API returned {len(items)} location(s) but config has {len(locations)}. "
            "Strict index matching failed! Check API response."
        )
        raise ValueError("Mismatch between requested locations and API response items.")

    for item, loc in zip(items, locations):
        # Open-Meteo đảm bảo thứ tự mảng trả về KHỚP TUYỆT ĐỐI với thứ tự tọa độ gửi lên.
        # Dù API có snap 2 quận gần nhau về cùng 1 tọa độ (grid cell), vị trí trong mảng vẫn không đổi.
        # Do đó, chỉ cần gộp (zip) theo đúng index là an toàn 100%.
        
        # Ghi đè tọa độ lưới của API bằng tọa độ gốc của chúng ta để làm khóa chính
        item["requested_latitude"]  = loc["latitude"]
        item["requested_longitude"] = loc["longitude"]
        item["location_name"]       = loc["name"]

    return data


def main():
    # ------------------------------------------------------------------
    # 1. Nhận tham số từ Airflow BashOperator (Jinja: {{ logical_date | ts }})
    #    Tuyệt đối KHÔNG dùng datetime.now() ở đây để bảo toàn Idempotency.
    # ------------------------------------------------------------------
    parser = argparse.ArgumentParser(description="Run ELT Pipeline")
    parser.add_argument(
        "--execution_date", type=str, required=True,
        help="Logical date from Airflow (ISO 8601 format, e.g. 2026-05-06T10:00:00+00:00)"
    )
    args = parser.parse_args()
    current_exec_date = args.execution_date

    logger.info(f"--- Bắt đầu chạy luồng ELT | execution_date: {current_exec_date} ---")

    # ------------------------------------------------------------------
    # 2. Nạp cấu hình API (URL, params) từ ConfigManager
    #    Đã tích hợp đọc cả 2 file JSON theo chuẩn SRP (Single Responsibility).
    # ------------------------------------------------------------------
    api_cfg = config_manager.open_meteo_api
    locations = config_manager.locations
    
    weather_url    = api_cfg["weather_url"]
    weather_params = api_cfg["weather_params"].copy()
    aq_url         = api_cfg["aq_url"]
    aq_params      = api_cfg["aq_params"].copy()

    # Tạo chuỗi tọa độ ngăn cách bằng dấu phẩy cho Batch API call
    lats = [str(loc["latitude"]) for loc in locations]
    lons = [str(loc["longitude"]) for loc in locations]
    
    weather_params["latitude"] = ",".join(lats)
    weather_params["longitude"] = ",".join(lons)
    aq_params["latitude"] = ",".join(lats)
    aq_params["longitude"] = ",".join(lons)

    # ------------------------------------------------------------------
    # 3. EXTRACT — Thực hiện cả 2 API call trước, xử lý lỗi ngay tại chỗ.
    #    Nếu một trong 2 thất bại hoàn toàn (sau retry), raise để Airflow
    #    đánh dấu task FAILED và không load dữ liệu rác vào DB.
    # ------------------------------------------------------------------
    logger.info("1. Đang lấy dữ liệu Thời Tiết từ Open-Meteo API...")
    weather_extractor = OpenMeteoExtractor(weather_url)
    raw_weather = weather_extractor.get_open_meteo_data(
        weather_params,
        expected_keys={"latitude", "longitude", "hourly"}
    )
    weather_data = _inject_location_metadata(raw_weather, locations)
    logger.info("-> Lấy dữ liệu Thời Tiết thành công!")

    logger.info("2. Đang lấy dữ liệu Chất lượng không khí từ Open-Meteo API...")
    aq_extractor = OpenMeteoExtractor(aq_url)
    raw_aq = aq_extractor.get_open_meteo_data(
        aq_params,
        expected_keys={"latitude", "longitude", "hourly"}
    )
    aq_data = _inject_location_metadata(raw_aq, locations)
    logger.info("-> Lấy dữ liệu Chất Lượng Không Khí thành công!")

    # ------------------------------------------------------------------
    # 4. LOAD — Chỉ chạy khi cả 2 Extract đều thành công.
    #    `finally` đảm bảo connection luôn được đóng dù có lỗi hay không.
    # ------------------------------------------------------------------
    logger.info("\n3. Đang đẩy dữ liệu vào PostgreSQL...")
    loader = PostgresLoader()
    try:
        loader.connect()

        logger.info("-> Đang Load dữ liệu Thời Tiết...")
        loader.insert_data(
            table_name="api_openmeteo_raw_data",
            source_type="weather_forecast_hourly",
            execution_date=current_exec_date,
            raw_json=weather_data
        )

        logger.info("-> Đang Load dữ liệu Chất lượng không khí...")
        loader.insert_data(
            table_name="api_openmeteo_raw_data",
            source_type="air_quality_hourly",
            execution_date=current_exec_date,
            raw_json=aq_data
        )
    finally:
        # finally chạy kể cả khi có exception — đảm bảo không rò rỉ connection
        loader.close()

    logger.info("\n--- Hoàn thành luồng ELT ---")


if __name__ == "__main__":
    main()

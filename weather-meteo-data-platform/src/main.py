import os
import sys
import json
import argparse

# Ensure the root of the project is in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.extractors.open_meteo import OpenMeteoExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.utils.logger import get_logger

logger = get_logger("MainPipeline")


def _load_api_config() -> dict:
    """
    Đọc config.json và trả về phần cấu hình Open-Meteo.
    Tách ra hàm riêng để dễ test và rõ ràng hơn.
    """
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)["api"]["open_meteo"]
    except FileNotFoundError:
        raise FileNotFoundError(f"API config file not found at: {config_path}")
    except (KeyError, json.JSONDecodeError) as e:
        raise ValueError(f"Invalid config.json structure: {e}") from e


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
    # 2. Nạp cấu hình API (URL, params) từ config.json
    #    ConfigManager chỉ quản lý runtime constants (timeout, retry).
    # ------------------------------------------------------------------
    api_cfg        = _load_api_config()
    weather_url    = api_cfg["weather_url"]
    weather_params = api_cfg["weather_params"]
    aq_url         = api_cfg["aq_url"]
    aq_params      = api_cfg["aq_params"]

    # ------------------------------------------------------------------
    # 3. EXTRACT — Thực hiện cả 2 API call trước, xử lý lỗi ngay tại chỗ.
    #    Nếu một trong 2 thất bại hoàn toàn (sau retry), raise để Airflow
    #    đánh dấu task FAILED và không load dữ liệu rác vào DB.
    # ------------------------------------------------------------------
    logger.info("1. Đang lấy dữ liệu Thời Tiết từ Open-Meteo API...")
    weather_extractor = OpenMeteoExtractor(weather_url)
    weather_data = weather_extractor.get_open_meteo_data(
        weather_params,
        expected_keys={"latitude", "longitude", "hourly"}
    )
    logger.info("-> Lấy dữ liệu Thời Tiết thành công!")

    logger.info("2. Đang lấy dữ liệu Chất lượng không khí từ Open-Meteo API...")
    aq_extractor = OpenMeteoExtractor(aq_url)
    aq_data = aq_extractor.get_open_meteo_data(
        aq_params,
        expected_keys={"latitude", "longitude", "hourly"}
    )
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

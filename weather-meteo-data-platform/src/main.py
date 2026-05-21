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


def _load_api_config() -> tuple:
    """
    Đọc config.json và trả về phần cấu hình Open-Meteo cùng danh sách locations.
    Tách ra hàm riêng để dễ test và rõ ràng hơn.
    """
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data["api"]["open_meteo"], data["locations"]
    except FileNotFoundError:
        raise FileNotFoundError(f"API config file not found at: {config_path}")
    except (KeyError, json.JSONDecodeError) as e:
        raise ValueError(f"Invalid config.json structure: {e}") from e


def _inject_location_metadata(data, locations: list):
    """
    Injects canonical/requested coordinates and names into the raw API response elements.
    This ensures weather and air quality datasets can be joined perfectly in the DB
    without grid snapping mismatches.

    LOGIC-C FIX: Open-Meteo Batch API không đảm bảo thứ tự response khớp với
    thứ tự request (API có thể sort/deduplicate coordinates). Match theo tọa độ
    thực tế của response (làm tròn 2 chữ số) thay vì match theo index.

    Side-effect note: Mutates dicts inside `data` in-place (intentional for performance).
    """
    # Nearest-neighbor matching trong bán kính 0.1° (Open-Meteo grid resolution)
    # Không thể dùng exact match vì:
    #   1. API snap tọa độ về grid riêng, không phải tọa độ trong config
    #   2. Nhiều config locations có thể cùng round-to-1 key (e.g. 21.0, 105.8)
    # Strategy: với mỗi API response item, tìm config location gần nhất trong 0.1°
    # Nếu 2 config locations cùng gần 1 API point → chọn cái gần nhất (Euclidean)

    items = data if isinstance(data, list) else [data]

    if len(items) != len(locations):
        logger.warning(
            f"API returned {len(items)} location(s) but config has {len(locations)}. "
            "Some locations may be merged by Open-Meteo (nearby stations). "
            "Check config.json for duplicate/overlapping coordinates."
        )

    MATCH_TOLERANCE = 0.15  # độ — ~15km, đủ rộng cho grid snapping nhưng không quá rộng

    unmatched = 0
    for item in items:
        api_lat = float(item.get("latitude", 0))
        api_lon = float(item.get("longitude", 0))

        # Tìm config location gần nhất trong tolerance
        best_match = None
        best_dist  = float("inf")
        for loc in locations:
            dist = ((loc["latitude"] - api_lat) ** 2 + (loc["longitude"] - api_lon) ** 2) ** 0.5
            if dist < best_dist:
                best_dist  = dist
                best_match = loc

        if best_match and best_dist <= MATCH_TOLERANCE:
            item["requested_latitude"]  = best_match["latitude"]
            item["requested_longitude"] = best_match["longitude"]
            item["location_name"]       = best_match["name"]
        else:
            item["requested_latitude"]  = api_lat
            item["requested_longitude"] = api_lon
            item["location_name"]       = f"UNKNOWN ({round(api_lat,2)},{round(api_lon,2)})"
            unmatched += 1
            logger.warning(
                f"No config match within {MATCH_TOLERANCE}° for API response at "
                f"({api_lat}, {api_lon}). Nearest config: {best_match['name'] if best_match else 'N/A'} "
                f"at dist={best_dist:.3f}°"
            )

    if unmatched > 0:
        logger.error(
            f"{unmatched} location(s) unmatched. Data loaded with UNKNOWN names. "
            "Consider reviewing config.json coordinates vs actual API responses."
        )

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
    # 2. Nạp cấu hình API (URL, params) từ config.json
    #    ConfigManager chỉ quản lý runtime constants (timeout, retry).
    # ------------------------------------------------------------------
    api_cfg, locations = _load_api_config()
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

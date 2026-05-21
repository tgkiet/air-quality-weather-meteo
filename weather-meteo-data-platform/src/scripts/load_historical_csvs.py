"""
load_historical_csvs.py — Nạp 2 file CSV lịch sử Hà Nội vào bronze_historical_weather.

File CSV cần được đặt vào: weather-meteo-data-platform/src/data/
  - hanoi_aq_weather_MERGED.csv       (2022-08-02 → 2025-10-19, ~874k dòng)
  - hanoi_realtime_data_updated.csv   (2025-10-20 → 2025-11-29, ~30k dòng)

Script tự động detect đường dẫn đúng khi chạy trong Docker hoặc trên Host.
Idempotent: chạy lại không tạo duplicate (ON CONFLICT DO UPDATE).
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.loaders.csv_loader import CSVLoader
from src.utils.logger import get_logger

logger = get_logger("LoadHistoricalCSV")

# ── Tên file CSV (cố định, không phụ thuộc environment) ────────────────────
CSV_MERGED   = "hanoi_aq_weather_MERGED.csv"
CSV_REALTIME = "hanoi_realtime_data_updated.csv"


def _resolve_csv_path(filename: str) -> str:
    """
    Tìm đường dẫn file CSV theo thứ tự ưu tiên:
    1. Biến môi trường CSV_DATA_DIR (flexible cho CI/CD, custom mount)
    2. src/data/ trong project (khi copy file vào src/data/ trước khi chạy)
    3. Đường dẫn Open-Meteo-Dataset trên host (development only)

    Raise FileNotFoundError nếu không tìm thấy ở cả 3 nơi.
    """
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

    candidates = [
        # 1. Env override (Docker volume mount hoặc CI/CD)
        os.path.join(os.environ.get("CSV_DATA_DIR", ""), filename),
        # 2. src/data/ trong project (khuyến nghị cho production)
        os.path.join(project_root, "src", "data", filename),
        # 3. Open-Meteo-Dataset trên host (development fallback)
        os.path.join(project_root, "..", "Open-Meteo-Dataset", filename),
    ]

    for path in candidates:
        if path and os.path.exists(path):
            return path

    raise FileNotFoundError(
        f"Không tìm thấy file '{filename}' ở bất kỳ đường dẫn nào.\n"
        f"Thử:\n"
        f"  1. Set env: CSV_DATA_DIR=/path/to/dir\n"
        f"  2. Copy file vào: {os.path.join(project_root, 'src', 'data', filename)}\n"
        f"Đường dẫn đã thử: {[p for p in candidates if p]}"
    )


def main():
    logger.info("=== BẮT ĐẦU NẠP DỮ LIỆU LỊCH SỬ HÀ NỘI TỪ CSV ===")

    loader = CSVLoader()
    try:
        loader.connect()
        loader.create_table_if_not_exists()

        total_loaded = 0

        for csv_name, required in [
            (CSV_MERGED,   True),   # Bắt buộc — file chính 2022-2025
            (CSV_REALTIME, False),  # Optional — có thể không có
        ]:
            try:
                csv_path = _resolve_csv_path(csv_name)
                logger.info(f"Nạp từ: {csv_path}")
                rows = loader.load_csv(csv_path)
                total_loaded += rows
                logger.info(f"  → {rows:,} dòng nạp thành công từ {csv_name}")
            except FileNotFoundError as e:
                if required:
                    logger.error(str(e))
                    raise  # File chính không có → fail toàn bộ
                else:
                    logger.warning(f"  → Không tìm thấy {csv_name}. Bỏ qua (optional).")

        logger.info(f"=== HOÀN THÀNH: tổng {total_loaded:,} dòng nạp vào Bronze ===")

    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng: {e}")
        sys.exit(1)
    finally:
        loader.close()


if __name__ == "__main__":
    main()

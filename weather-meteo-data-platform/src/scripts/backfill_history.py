"""
backfill_history.py — Backfill dữ liệu lịch sử Weather & Air Quality từ Open-Meteo Archive API.

Dùng cho 2 trường hợp:
  1. HCM full backfill (không có CSV):
     python3 backfill_history.py --location-prefix HCM \
         --start-date 2022-08-02 --end-date 2026-05-19

  2. HN gap fill (sau khi đã nạp CSV đến 2025-11-29):
     python3 backfill_history.py --location-prefix HN \
         --start-date 2025-11-30 --end-date 2026-05-19

Idempotency: Script có thể chạy lại bất kỳ lúc nào mà không tạo duplicate
             (ON CONFLICT DO UPDATE tại bronze_historical_weather).
"""

import os
import sys
import json
import time
import argparse
import requests
from psycopg2.extras import execute_values
from psycopg2 import Error

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.loaders.base_loader import BasePostgresLoader
from src.utils.logger import get_logger

logger = get_logger("HistoricalBackfiller")


class HistoricalBackfiller(BasePostgresLoader):
    """
    Backfill dữ liệu lịch sử Weather + Air Quality cho một nhóm locations
    (filter theo prefix tên: "HCM " hoặc "HN ") từ Open-Meteo Archive API.
    """

    def __init__(self, start_date: str, end_date: str, location_prefix: str):
        super().__init__()
        self.start_date      = start_date
        self.end_date        = end_date
        self.location_prefix = location_prefix  # "HCM " hoặc "HN "

    def run_backfill(self):
        # ──────────────────────────────────────────────────────
        # 1. Load config và filter locations theo prefix
        # ──────────────────────────────────────────────────────
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "config", "config.json"
        )
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config_data = json.load(f)
                all_locations = config_data["locations"]
        except Exception as e:
            logger.error(f"Failed to load config.json: {e}")
            raise  # Propagate — không để script exit im lặng

        target_locations = [
            loc for loc in all_locations
            if loc["name"].startswith(self.location_prefix)
        ]

        if not target_locations:
            raise ValueError(
                f"Không tìm thấy location nào với prefix '{self.location_prefix}' "
                f"trong config.json. Kiểm tra lại --location-prefix."
            )

        logger.info(
            f"Backfill config: prefix='{self.location_prefix}' | "
            f"locations={len(target_locations)} | "
            f"range={self.start_date} → {self.end_date}"
        )

        # ──────────────────────────────────────────────────────
        # 2. Connect DB + loop từng location
        # ──────────────────────────────────────────────────────
        self.connect()

        # location_id offset: tránh xung đột với ID của CSV Hà Nội
        # CSV HN dùng ID thực từ OpenAQ (vd: 2539). HCM dùng 3000000+, HN gap 4000000+.
        id_offset = 3000000 if self.location_prefix == "HCM " else 4000000

        success_count = 0
        fail_count    = 0

        for idx, loc in enumerate(target_locations):
            location_id = id_offset + idx
            name        = loc["name"]
            lat         = loc["latitude"]
            lon         = loc["longitude"]

            logger.info(
                f"[{idx+1}/{len(target_locations)}] {name} "
                f"(lat={lat}, lon={lon}) | id={location_id}"
            )

            try:
                weather_data = self._fetch_weather_history(lat, lon)
                time.sleep(1.0)  # polite delay tránh rate limit

                aq_data = self._fetch_aq_history(lat, lon)
                time.sleep(1.0)

                if not weather_data:
                    logger.error(f"  ✗ Weather fetch failed for {name}. Skipping.")
                    fail_count += 1
                    continue

                # AQ có thể None nếu không có data cho khu vực này — vẫn load weather
                self._merge_and_load(location_id, lat, lon, name, weather_data, aq_data or {})
                logger.info(f"  ✓ Loaded {name}.")
                success_count += 1

            except Exception as e:
                logger.error(f"  ✗ Error processing {name}: {e}")
                fail_count += 1
                continue  # Skip location này, tiếp tục location khác

        self.close()
        logger.info(
            f"Backfill complete: {success_count}/{len(target_locations)} locations OK, "
            f"{fail_count} failed."
        )
        if fail_count > 0:
            logger.warning(
                f"{fail_count} location(s) failed. Chạy lại script là idempotent "
                f"(ON CONFLICT DO UPDATE) — các location đã thành công sẽ không bị duplicate."
            )

    # ──────────────────────────────────────────────────────────────
    # PRIVATE: Fetch Methods
    # ──────────────────────────────────────────────────────────────

    def _fetch_weather_history(self, lat, lon):
        """
        Gọi Open-Meteo Archive API để lấy dữ liệu thời tiết lịch sử.
        Trả về dict hourly hoặc None nếu thất bại sau 3 lần retry.
        """
        url    = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude":   lat,
            "longitude":  lon,
            "start_date": self.start_date,
            "end_date":   self.end_date,
            "hourly":     "temperature_2m,relative_humidity_2m,precipitation,rain,"
                          "wind_speed_10m,wind_direction_10m,pressure_msl",
            "timezone":   "Asia/Bangkok"
        }
        return self._fetch_with_retry(url, params, label="Weather")

    def _fetch_aq_history(self, lat, lon):
        """
        Gọi Open-Meteo Air Quality API để lấy lịch sử chất lượng không khí.
        Trả về dict hourly hoặc None nếu thất bại sau 3 lần retry.
        AQ API có thể không có data cho một số khu vực → caller xử lý None.
        """
        url    = "https://air-quality-api.open-meteo.com/v1/air-quality"
        params = {
            "latitude":   lat,
            "longitude":  lon,
            "start_date": self.start_date,
            "end_date":   self.end_date,
            "hourly":     "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone",
            "timezone":   "Asia/Bangkok"
        }
        return self._fetch_with_retry(url, params, label="AirQuality")

    def _fetch_with_retry(self, url: str, params: dict, label: str, max_retries: int = 3):
        """
        Generic HTTP GET với exponential backoff retry.
        Retry cho: network errors và HTTP 429/5xx.
        Không retry cho: HTTP 4xx (lỗi client).
        """
        for attempt in range(max_retries):
            try:
                r = requests.get(url, params=params, timeout=60)
                if r.status_code == 200:
                    return r.json().get("hourly", {})
                elif r.status_code == 429 or r.status_code >= 500:
                    # Rate limit hoặc server error → có thể retry
                    logger.warning(
                        f"  {label} API status {r.status_code}. "
                        f"Attempt {attempt+1}/{max_retries}. Retrying..."
                    )
                else:
                    # 4xx → lỗi client, không retry
                    logger.error(
                        f"  {label} API status {r.status_code} (client error). "
                        f"URL: {r.url[:80]}. Skipping."
                    )
                    return None
            except Exception as e:
                logger.warning(
                    f"  {label} API error: {e}. "
                    f"Attempt {attempt+1}/{max_retries}. Retrying..."
                )
            time.sleep(2 ** attempt)  # 1s → 2s → 4s exponential backoff
        logger.error(f"  {label} API failed after {max_retries} attempts.")
        return None

    # ──────────────────────────────────────────────────────────────
    # PRIVATE: Merge & Load
    # ──────────────────────────────────────────────────────────────

    def _merge_and_load(self, location_id, lat, lon, location_name, weather, aq):
        """
        Gộp Weather + AQ theo time key, sau đó UPSERT vào bronze_historical_weather.

        LOGIC-A FIX: Align AQ theo time_str dict lookup (không phải positional index).
        Open-Meteo Archive API trả về Weather và AQ độc lập — time arrays có thể
        bắt đầu từ offset khác nhau hoặc bị thiếu giờ.

        BUG-1 FIX: safe_get guard IndexError khi array ngắn hơn time array.
        """
        times = weather.get("time", [])
        if not times:
            logger.warning(f"  No time data in weather response. Skipping {location_name}.")
            return

        def safe_get(arr, idx):
            """Trả về arr[idx] hoặc None nếu arr None/rỗng/idx out-of-range."""
            if arr is None:
                return None
            return arr[idx] if idx is not None and idx < len(arr) else None

        # Build AQ time→index mapping để align by time key
        aq_times        = aq.get("time", []) if aq else []
        aq_time_to_idx  = {t: i for i, t in enumerate(aq_times)}

        insert_rows = []
        for i, time_str in enumerate(times):
            aq_idx = aq_time_to_idx.get(time_str)  # None nếu AQ không có giờ này
            row = (
                time_str,   # cast bởi SQL template: %s::TIMESTAMP AT TIME ZONE 'Asia/Bangkok'
                safe_get(weather.get("temperature_2m"),       i),
                safe_get(weather.get("relative_humidity_2m"), i),
                safe_get(weather.get("precipitation"),        i),
                safe_get(weather.get("rain"),                 i),
                safe_get(weather.get("wind_speed_10m"),       i),
                safe_get(weather.get("wind_direction_10m"),   i),
                safe_get(weather.get("pressure_msl"),         i),
                safe_get(aq.get("pm10"),              aq_idx),
                safe_get(aq.get("pm2_5"),             aq_idx),
                safe_get(aq.get("carbon_monoxide"),   aq_idx),
                safe_get(aq.get("nitrogen_dioxide"),  aq_idx),
                safe_get(aq.get("sulphur_dioxide"),   aq_idx),
                safe_get(aq.get("ozone"),             aq_idx),
                location_id,
                lat,
                lon,
                location_name,
            )
            insert_rows.append(row)

        if not insert_rows:
            logger.warning(f"  No rows to insert for {location_name}.")
            return

        query = """
            INSERT INTO bronze_historical_weather (
                datetime, temperature_2m, relative_humidity_2m, precipitation, rain,
                wind_speed_10m, wind_direction_10m, pressure_msl,
                pm10_cams, pm2_5_cams, carbon_monoxide_cams, nitrogen_dioxide_cams,
                sulphur_dioxide_cams, ozone_cams, location_id, lat, lon, location_name
            ) VALUES %s
            ON CONFLICT (datetime, lat, lon) DO UPDATE SET
                temperature_2m         = EXCLUDED.temperature_2m,
                relative_humidity_2m   = EXCLUDED.relative_humidity_2m,
                precipitation          = EXCLUDED.precipitation,
                rain                   = EXCLUDED.rain,
                wind_speed_10m         = EXCLUDED.wind_speed_10m,
                wind_direction_10m     = EXCLUDED.wind_direction_10m,
                pressure_msl           = EXCLUDED.pressure_msl,
                pm10_cams              = EXCLUDED.pm10_cams,
                pm2_5_cams             = EXCLUDED.pm2_5_cams,
                carbon_monoxide_cams   = EXCLUDED.carbon_monoxide_cams,
                nitrogen_dioxide_cams  = EXCLUDED.nitrogen_dioxide_cams,
                sulphur_dioxide_cams   = EXCLUDED.sulphur_dioxide_cams,
                ozone_cams             = EXCLUDED.ozone_cams,
                location_id            = EXCLUDED.location_id,
                location_name          = EXCLUDED.location_name;
        """
        # BUG-2 FIX: Explicit template cast ::TIMESTAMP AT TIME ZONE 'Asia/Bangkok'
        # → Postgres chuyển giờ địa phương về TIMESTAMPTZ (UTC base) đúng chuẩn
        template = (
            "(%s::TIMESTAMP AT TIME ZONE 'Asia/Bangkok', "
            "%s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s)"
        )
        try:
            with self.connection.cursor() as cursor:
                execute_values(cursor, query, insert_rows, template=template)
            self.connection.commit()
            logger.info(f"  Upserted {len(insert_rows)} rows for {location_name}.")
        except Error as e:
            self.connection.rollback()
            logger.error(f"  DB error for {location_name}: {e}")
            raise


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Backfill lịch sử Weather & Air Quality từ Open-Meteo Archive API.\n"
            "\n"
            "Ví dụ:\n"
            "  # HCM — toàn bộ từ 2022\n"
            "  python3 backfill_history.py --location-prefix HCM "
            "--start-date 2022-08-02 --end-date 2026-05-19\n"
            "\n"
            "  # HN — chỉ gap sau khi nạp CSV (CSV đến 2025-11-29)\n"
            "  python3 backfill_history.py --location-prefix HN "
            "--start-date 2025-11-30 --end-date 2026-05-19\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--location-prefix",
        type=str,
        required=True,
        choices=["HCM", "HN"],
        help="Nhóm locations cần backfill: 'HCM' (22 quận) hoặc 'HN' (31 trạm)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Ngày bắt đầu (YYYY-MM-DD). VD: 2022-08-02"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="Ngày kết thúc (YYYY-MM-DD). VD: 2026-05-19"
    )
    args = parser.parse_args()

    # Thêm dấu cách để khớp với prefix trong config.json ("HCM Quận 1", "HN Đống Đa")
    prefix = args.location_prefix + " "

    logger.info(
        f"=== Historical Backfill START ===\n"
        f"    Location prefix : '{prefix}'\n"
        f"    Date range      : {args.start_date} → {args.end_date}"
    )

    backfiller = HistoricalBackfiller(
        start_date=args.start_date,
        end_date=args.end_date,
        location_prefix=prefix,
    )
    backfiller.run_backfill()


if __name__ == "__main__":
    main()

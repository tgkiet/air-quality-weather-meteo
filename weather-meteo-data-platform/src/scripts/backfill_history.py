"""
backfill_history.py — Backfill dữ liệu lịch sử Weather & Air Quality từ Open-Meteo Archive API.

Dùng để khởi tạo dữ liệu lịch sử cho toàn bộ 52 Vùng quan trắc (30 HN + 22 HCM):

     python3 backfill_history.py --location-prefix HN \
         --start-date 2022-08-02 --end-date 2026-05-27

     python3 backfill_history.py --location-prefix HCM \
         --start-date 2022-08-02 --end-date 2026-05-27

Idempotency: Script có thể chạy lại bất kỳ lúc nào mà không tạo duplicate
             (ON CONFLICT DO UPDATE tại bronze_historical_weather).
"""

import os
import sys
import json
import time
import argparse
import requests
from datetime import date as _date, datetime as _datetime, timedelta as _timedelta
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
        self.session         = requests.Session()

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



        id_offset = 3000000 if self.location_prefix == "HCM " else 4000000

        success_count = 0
        fail_count    = 0

        # try/finally đảm bảo self.close() luôn được gọi kể cả khi
        # KeyboardInterrupt (Ctrl+C) hoặc SystemExit xảy ra giữa chừng.
        # Lý do cần thiết: script có thể chạy hàng giờ, user Ctrl+C là thực tế.
        # KeyboardInterrupt/SystemExit không phải subclass của Exception →
        # không bị catch bởi inner `except Exception` trong loop.
        try:
            for idx, loc in enumerate(target_locations):
                location_id = id_offset + idx
                name        = loc["name"]
                lat         = loc["latitude"]
                lon         = loc["longitude"]

                logger.info(
                    f"[{idx+1}/{len(target_locations)}] {name} "
                    f"(lat={lat}, lon={lon}) | id={location_id}"
                )

                # Smart Skip: Tránh tốn API token nếu location đã được backfill đầy đủ
                # CHO ĐÚNG DATE RANGE được request.
                #
                # BUG CŨ: Chỉ đếm tổng row của location → bỏ qua backfill dù date range mới
                #          chưa có data (ví dụ: tắt hệ thống 1 tháng → thiếu dữ liệu tháng đó).
                #
                # FIX: Đếm row trong khoảng [start_date, end_date] cụ thể, so với số giờ
                #      lý thuyết. Nếu đã đủ (>= 95% giờ lý thuyết) → skip.
                #      row_count=0 là default an toàn: nếu connection fail, vẫn fetch API.
                skip_this_location = False
                if getattr(self, "connection", None) and not self.connection.closed:
                    try:
                        # Số giờ lý thuyết trong range [start_date, end_date] (inclusive)
                        start_d = _date.fromisoformat(self.start_date)
                        end_d   = _date.fromisoformat(self.end_date)
                        expected_hours = (end_d - start_d).days * 24 + 24  # inclusive end date

                        with self.connection.cursor() as cursor:
                            cursor.execute(
                                """
                                SELECT COUNT(*) FROM bronze_historical_weather
                                WHERE location_id = %s
                                  AND datetime >= %s::TIMESTAMP AT TIME ZONE 'Asia/Bangkok'
                                  AND datetime <  (%s::DATE + INTERVAL '1 day')::TIMESTAMP AT TIME ZONE 'Asia/Bangkok'
                                """,
                                (location_id, self.start_date, self.end_date)
                            )
                            row_count_in_range = cursor.fetchone()[0]

                        # Cho phép thiếu tối đa 5% (missing hours từ API)
                        threshold = int(expected_hours * 0.95)
                        if row_count_in_range >= threshold:
                            logger.info(
                                f"  ✓ Already backfilled for requested range "
                                f"({row_count_in_range}/{expected_hours} hours). Skipping API calls."
                            )
                            skip_this_location = True
                        else:
                            logger.info(
                                f"  ↻ Incomplete for requested range "
                                f"({row_count_in_range}/{expected_hours} hours present). "
                                f"Will fetch from API."
                            )
                    except Exception as skip_err:
                        logger.warning(f"  Skip-check failed for {name}: {skip_err}. Will fetch from API.")
                        skip_this_location = False

                if skip_this_location:
                    success_count += 1
                    continue

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

            logger.info(
                f"Backfill complete: {success_count}/{len(target_locations)} locations OK, "
                f"{fail_count} failed."
            )
            if fail_count > 0:
                logger.warning(
                    f"{fail_count} location(s) failed. Chạy lại script là idempotent "
                    f"(ON CONFLICT DO UPDATE) — các location đã thành công sẽ không bị duplicate."
                )
        finally:
            self.close()

    # ──────────────────────────────────────────────────────────────
    # PRIVATE: Fetch Methods
    # ──────────────────────────────────────────────────────────────

    def _fetch_weather_history(self, lat, lon):
        """
        Gọi Open-Meteo Archive API để lấy dữ liệu thời tiết lịch sử.
        Trả về dict hourly hoặc None nếu thất bại sau {max_retries} lần retry.
        """
        url    = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude":   lat,
            "longitude":  lon,
            "start_date": self.start_date,
            "end_date":   self.end_date,
            # NOTE: uv_index không có trên Archive API — chỉ có trên Forecast API.
            # Mọi giá trị nạp vào DB sẽ là NULL 100%. Đã loại bỏ khỏi request.
            "hourly":     "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,"
                          "precipitation,rain,pressure_msl,surface_pressure,cloud_cover,"
                          "wind_speed_10m,wind_direction_10m,wind_gusts_10m",
            "timezone":   "Asia/Bangkok"
        }
        return self._fetch_with_retry(url, params, label="Weather")

    def _fetch_aq_history(self, lat, lon):
        """
        Gọi Open-Meteo Air Quality API để lấy lịch sử chất lượng không khí.
        Trả về dict hourly hoặc None nếu thất bại sau max_retries lần retry.
        AQ API có thể không có data cho một số khu vực → caller xử lý None.
        """
        url    = "https://air-quality-api.open-meteo.com/v1/air-quality"
        params = {
            "latitude":   lat,
            "longitude":  lon,
            "start_date": self.start_date,
            "end_date":   self.end_date,
            "hourly":     "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,aerosol_optical_depth,dust,uv_index",
            "timezone":   "Asia/Bangkok"
        }
        return self._fetch_with_retry(url, params, label="AirQuality")

    def _fetch_with_retry(self, url: str, params: dict, label: str, max_retries: int = 5):
        """
        Generic HTTP GET với exponential backoff retry.
        Retry cho: network errors và HTTP 429/5xx.
        Không retry cho: HTTP 4xx (lỗi client).
        """
        for attempt in range(max_retries):
            try:
                r = self.session.get(url, params=params, timeout=60)
                if r.status_code == 200:
                    return r.json().get("hourly", {})
                elif r.status_code == 429 or r.status_code >= 500:
                    # Phân biệt Daily Limit (terminal) vs Hourly Limit (transient)
                    try:
                        reason = r.json().get("reason", "")
                    except Exception:
                        reason = r.text[:100]
                    if "daily" in reason.lower():
                        logger.critical(
                            f"  Daily API limit reached: {reason}. "
                            f"Stopping — please retry after 07:00 ICT."
                        )
                        raise SystemExit(2)
                    logger.warning(
                        f"  {label} API {r.status_code}: {reason}. "
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

        Align AQ theo time_str dict lookup (không phải positional index).
        Open-Meteo Archive API trả về Weather và AQ độc lập — time arrays có thể
        bắt đầu từ offset khác nhau hoặc bị thiếu giờ.

        safe_get guard IndexError khi array ngắn hơn time array.
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
                safe_get(weather.get("dew_point_2m"),         i),
                safe_get(weather.get("apparent_temperature"), i),
                safe_get(weather.get("precipitation"),        i),
                safe_get(weather.get("rain"),                 i),
                safe_get(weather.get("pressure_msl"),         i),
                safe_get(weather.get("surface_pressure"),     i),
                safe_get(weather.get("cloud_cover"),          i),
                safe_get(weather.get("wind_speed_10m"),       i),
                safe_get(weather.get("wind_direction_10m"),   i),
                safe_get(weather.get("wind_gusts_10m"),       i),
                None,                                             # uv_index: Archive Weather API không cung cấp trường này
                safe_get(aq.get("pm10"),                  aq_idx),
                safe_get(aq.get("pm2_5"),                 aq_idx),
                safe_get(aq.get("carbon_monoxide"),       aq_idx),
                safe_get(aq.get("nitrogen_dioxide"),      aq_idx),
                safe_get(aq.get("sulphur_dioxide"),       aq_idx),
                safe_get(aq.get("ozone"),                 aq_idx),
                safe_get(aq.get("aerosol_optical_depth"), aq_idx),
                safe_get(aq.get("dust"),                  aq_idx),
                safe_get(aq.get("uv_index"),              aq_idx),
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
                datetime, temperature_2m, relative_humidity_2m, dew_point_2m, apparent_temperature,
                precipitation, rain, pressure_msl, surface_pressure, cloud_cover,
                wind_speed_10m, wind_direction_10m, wind_gusts_10m, uv_index,
                pm10_cams, pm2_5_cams, carbon_monoxide_cams, nitrogen_dioxide_cams,
                sulphur_dioxide_cams, ozone_cams, aerosol_optical_depth_cams, dust_cams, aq_uv_index_cams,
                location_id, lat, lon, location_name
            ) VALUES %s
            ON CONFLICT (datetime, lat, lon) DO UPDATE SET
                temperature_2m         = EXCLUDED.temperature_2m,
                relative_humidity_2m   = EXCLUDED.relative_humidity_2m,
                dew_point_2m           = EXCLUDED.dew_point_2m,
                apparent_temperature   = EXCLUDED.apparent_temperature,
                precipitation          = EXCLUDED.precipitation,
                rain                   = EXCLUDED.rain,
                pressure_msl           = EXCLUDED.pressure_msl,
                surface_pressure       = EXCLUDED.surface_pressure,
                cloud_cover            = EXCLUDED.cloud_cover,
                wind_speed_10m         = EXCLUDED.wind_speed_10m,
                wind_direction_10m     = EXCLUDED.wind_direction_10m,
                wind_gusts_10m         = EXCLUDED.wind_gusts_10m,
                uv_index               = EXCLUDED.uv_index,
                pm10_cams              = EXCLUDED.pm10_cams,
                pm2_5_cams             = EXCLUDED.pm2_5_cams,
                carbon_monoxide_cams   = EXCLUDED.carbon_monoxide_cams,
                nitrogen_dioxide_cams  = EXCLUDED.nitrogen_dioxide_cams,
                sulphur_dioxide_cams   = EXCLUDED.sulphur_dioxide_cams,
                ozone_cams             = EXCLUDED.ozone_cams,
                aerosol_optical_depth_cams = EXCLUDED.aerosol_optical_depth_cams,
                dust_cams              = EXCLUDED.dust_cams,
                aq_uv_index_cams       = EXCLUDED.aq_uv_index_cams,
                location_id            = EXCLUDED.location_id,
                location_name          = EXCLUDED.location_name;
        """
        # Explicit template cast ::TIMESTAMP AT TIME ZONE 'Asia/Bangkok'
        # → Postgres chuyển giờ địa phương về TIMESTAMPTZ (UTC base) đúng chuẩn
        template = (
            "(%s::TIMESTAMP AT TIME ZONE 'Asia/Bangkok', "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s)"
        )
        try:
            with self.connection.cursor() as cursor:
                execute_values(cursor, query, insert_rows, template=template, page_size=1000)
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
            "Ví dụ (HN — 30 Quận/Huyện):\n"
            "  python3 src/scripts/backfill_history.py \\\n"
            "            --location-prefix HN \\\n"
            "            --start-date 2022-08-02 --end-date 2026-05-27\n\n"
            "Ví dụ (HCM — 22 Quận/Huyện):\n"
            "  python3 src/scripts/backfill_history.py \\\n"
            "            --location-prefix HCM \\\n"
            "            --start-date 2022-08-02 --end-date 2026-05-27\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--location-prefix",
        type=str,
        required=True,
        choices=["HCM", "HN"],
        help="Nhóm locations cần backfill: 'HCM' (22 locations) hoặc 'HN' (30 locations)"
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
        help="Ngày kết thúc (YYYY-MM-DD). VD: 2026-05-27"
    )
    args = parser.parse_args()

    # Validate định dạng và thứ tự ngày trước khi làm bất cứ điều gì.
    # Fail-fast tại đây rõ ràng hơn là để API trả về empty data sau nhiều giây.
    try:
        start_d = _date.fromisoformat(args.start_date)
        end_d   = _date.fromisoformat(args.end_date)
    except ValueError as e:
        parser.error(f"Định dạng ngày không hợp lệ (cần YYYY-MM-DD): {e}")
    if start_d > end_d:
        parser.error(
            f"--start-date ({args.start_date}) phải <= --end-date ({args.end_date})."
        )

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

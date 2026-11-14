import psycopg2
from contextlib import contextmanager
import psycopg2.extras
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger("BotServices")
FRESHNESS_HOURS = 3

# Each metric must match its own forecast batch in Bronze. This also supports
# warehouses that have not yet rebuilt Gold with its new provenance columns.
WEATHER_BATCH_JOIN = """
    LEFT JOIN public.api_openmeteo_raw_data AS weather_batch
           ON weather_batch.source_type = 'weather_forecast_hourly'
          AND weather_batch.execution_date = g.execution_date
"""
AQ_BATCH_JOIN = """
    LEFT JOIN silver_layer.slv_air_quality_hourly AS aq
           ON (aq.forecast_time, aq.latitude, aq.longitude) =
              (g.forecast_time, g.latitude, g.longitude)
    LEFT JOIN public.api_openmeteo_raw_data AS aq_batch
           ON aq_batch.id = aq.raw_id AND aq_batch.execution_date = aq.execution_date
          AND aq_batch.source_type = 'air_quality_hourly'
"""
WEATHER_FRESH = f"""(
    weather_batch.ingested_at >= now() - interval '{FRESHNESS_HOURS} hours'
    AND weather_batch.execution_date >= now() - interval '{FRESHNESS_HOURS} hours'
)"""
AQ_FRESH = f"""(
    aq_batch.ingested_at >= now() - interval '{FRESHNESS_HOURS} hours'
    AND aq_batch.execution_date >= now() - interval '{FRESHNESS_HOURS} hours'
)"""

class BotDatabaseManager:
    """Handles all database interactions for the Telegram Bot."""
    def __init__(self, loader_instance):
        self.loader = loader_instance
        # QUALITY-7 FIX: Removed ThreadedConnectionPool. 
        # Keeping idle connections in a pool causes TCP timeouts (stale connections) 
        # when the bot is idle, leading to 3-second UI lags on the first click.
        # Direct connections on localhost take <10ms, eliminating the stale connection bug entirely.

    @contextmanager
    def get_db_connection(self):
        conn = None
        try:
            conn = psycopg2.connect(
                dbname=self.loader.db_name,
                user=self.loader.db_user,
                password=self.loader.db_password,
                host=self.loader.db_host,
                port=self.loader.db_port,
                connect_timeout=5,
            )
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            if conn and not conn.closed:
                conn.rollback()
            raise
        finally:
            if conn and not conn.closed:
                conn.close()

    def init_lang_table(self):
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS silver_layer.bot_user_preferences (
                            chat_id    BIGINT     PRIMARY KEY,
                            language   VARCHAR(2) NOT NULL DEFAULT 'en'
                                       CHECK (language IN ('en', 'vi')),
                            updated_at TIMESTAMP  NOT NULL DEFAULT NOW()
                        );
                    """)
                conn.commit()
        except Exception as e:
            logger.warning(f"Could not init preferences table: {e}")

    def get_user_lang(self, chat_id: int) -> str:
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT language FROM silver_layer.bot_user_preferences WHERE chat_id = %s",
                        (chat_id,)
                    )
                    row = cur.fetchone()
                    return row[0] if row else "en"
        except Exception as e:
            logger.error(f"Error getting user lang: {e}")
            return "en"

    def set_user_lang(self, chat_id: int, lang: str):
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO silver_layer.bot_user_preferences (chat_id, language, updated_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (chat_id)
                        DO UPDATE SET language = EXCLUDED.language, updated_at = NOW();
                    """, (chat_id, lang))
                conn.commit()
        except Exception as e:
            logger.error(f"Error saving user lang: {e}")

    def get_home_location(self, chat_id: int) -> str | None:
        with self.get_db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT home_location FROM silver_layer.bot_user_preferences WHERE chat_id = %s",
                (chat_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def set_home_location(self, chat_id: int, location: str) -> None:
        with self.get_db_connection() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO silver_layer.bot_user_preferences (chat_id, home_location)
                VALUES (%s, %s)
                ON CONFLICT (chat_id) DO UPDATE
                SET home_location = EXCLUDED.home_location, updated_at = NOW()
            """, (chat_id, location))
            conn.commit()

    def set_subscription(self, chat_id: int, enabled: bool) -> bool:
        with self.get_db_connection() as conn, conn.cursor() as cur:
            cur.execute("""
                UPDATE silver_layer.bot_user_preferences
                SET daily_briefing = %s, updated_at = NOW()
                WHERE chat_id = %s AND home_location IS NOT NULL
            """, (enabled, chat_id))
            updated = cur.rowcount == 1
            conn.commit()
            return updated

    def query_weather(self, db_name: str, limit: int = 6, offset: int = 0) -> list:
        with self.get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"""
                SELECT g.forecast_time, g.temperature_2m, g.precipitation_probability,
                       g.precipitation, g.wind_speed_10m, g.weather_uv_index,
                       weather_batch.ingested_at
                FROM gold_layer.mart_hourly_conditions AS g
                {WEATHER_BATCH_JOIN}
                WHERE g.location_name = %s AND {WEATHER_FRESH}
                  AND g.forecast_time >= NOW() + %s * interval '1 hour'
                  AND g.forecast_time < NOW() + (%s + %s) * interval '1 hour'
                ORDER BY g.forecast_time;
            """, (db_name, offset, offset, limit))
            return cur.fetchall()

    def query_aqi(self, db_name: str) -> dict | None:
        with self.get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT aq.forecast_time, aq.pm2_5, aq.pm10, aq.nitrogen_dioxide, aq.ozone,
                       aq_batch.ingested_at,
                       COALESCE({AQ_FRESH} AND aq.forecast_time >= now() - interval '2 hours', FALSE) AS fresh
                FROM silver_layer.slv_air_quality_hourly AS aq
                LEFT JOIN public.api_openmeteo_raw_data AS aq_batch
                       ON aq_batch.id = aq.raw_id AND aq_batch.execution_date = aq.execution_date
                      AND aq_batch.source_type = 'air_quality_hourly'
                WHERE aq.location_name = %s
                  AND aq.forecast_time >= NOW() - interval '24 hours'
                  AND aq.forecast_time <= NOW() + interval '1 hour'
                ORDER BY fresh DESC, (aq.pm2_5 IS NOT NULL) DESC, aq.forecast_time DESC
                LIMIT 1;
            """, (db_name,))
            return cur.fetchone()

    def query_briefing(self, db_name: str) -> list:
        with self.get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT g.forecast_time,
                       g.forecast_time < now() + interval '6 hours' AS within_6h,
                       CASE WHEN {WEATHER_FRESH} THEN g.temperature_2m END AS temperature_2m,
                       CASE WHEN {WEATHER_FRESH} THEN g.precipitation END AS precipitation,
                       CASE WHEN {WEATHER_FRESH} THEN g.precipitation_probability END AS precipitation_probability,
                       CASE WHEN {WEATHER_FRESH} THEN weather_batch.ingested_at END AS weather_ingested_at,
                       CASE WHEN {AQ_FRESH} THEN aq.pm2_5 END AS pm2_5,
                       CASE WHEN {AQ_FRESH} THEN aq_batch.ingested_at END AS aq_ingested_at
                FROM gold_layer.mart_hourly_conditions AS g
                {WEATHER_BATCH_JOIN}
                {AQ_BATCH_JOIN}
                WHERE g.location_name = %s AND g.forecast_time >= NOW()
                  AND g.forecast_time < NOW() + interval '24 hours'
                ORDER BY g.forecast_time;
            """, (db_name,))
            return cur.fetchall()


class BotFormatter:
    """Handles text formatting and insight generation for the Telegram Bot."""
    def __init__(self, rain_prob_threshold, rain_mm_threshold, pm25_alert_threshold, bkk_tz):
        self.rain_prob_threshold = rain_prob_threshold
        self.rain_mm_threshold = rain_mm_threshold
        self.pm25_alert_threshold = pm25_alert_threshold
        self.bkk_tz = bkk_tz

    def _hour(self, value: datetime) -> str:
        return value.astimezone(self.bkk_tz).strftime("%H:%M %d/%m/%Y")

    def fmt_briefing(self, label: str, rows: list, lang: str) -> str:
        en = lang == "en"
        title = f"24-HOUR BRIEFING · {label}" if en else f"BẢN TIN 24 GIỜ · {label}"
        missing = "Insufficient current forecast data" if en else "Chưa đủ dữ liệu dự báo mới"
        weather_times = [r["weather_ingested_at"] for r in rows if r["weather_ingested_at"]]
        aq_times = [r["aq_ingested_at"] for r in rows if r["aq_ingested_at"]]

        rain = [r for r in rows if r["within_6h"] and r["precipitation"] is not None
                and r["precipitation_probability"] is not None]
        temperatures = [r["temperature_2m"] for r in rows if r["temperature_2m"] is not None]
        pm25 = [r for r in rows if r["pm2_5"] is not None]

        rain_label = "Rain (6h)" if en else "Mưa (6 giờ)"
        temp_label = "Temperature (24h)" if en else "Nhiệt độ (24 giờ)"
        pm25_label = "Peak PM2.5 (24h)" if en else "PM2.5 cao nhất (24 giờ)"
        rain_summary = missing
        if rain:
            peak = max(rain, key=lambda r: (r["precipitation"], r["precipitation_probability"]))
            rain_summary = f'{peak["precipitation"]:.1f} mm, {peak["precipitation_probability"]}% · {self._hour(peak["forecast_time"])}'
        temp_summary = f"{min(temperatures):.1f}–{max(temperatures):.1f} °C" if temperatures else missing
        pm25_summary = missing
        if pm25:
            peak = max(pm25, key=lambda r: r["pm2_5"])
            pm25_summary = f'{peak["pm2_5"]:.1f} µg/m³ · {self._hour(peak["forecast_time"])}'
        updated = "Latest fetch" if en else "Lấy dữ liệu gần nhất"
        hour_count = "h" if en else "giờ"
        lines = [
            title,
            f"{rain_label}: {rain_summary} ({len(rain)}/6 {hour_count})",
            f"{temp_label}: {temp_summary} ({len(temperatures)}/24 {hour_count})",
            f"{pm25_label}: {pm25_summary} ({len(pm25)}/24 {hour_count})",
        ]
        if weather_times:
            lines.append(f"Open-Meteo · {updated} {self._hour(max(weather_times))}")
        if aq_times:
            lines.append(f"Open-Meteo/CAMS · {updated} {self._hour(max(aq_times))}")
        return "\n".join(lines)

    def fmt_weather(self, label: str, rows: list, lang: str, limit: int = 6, offset: int = 0) -> str:
        en = lang == "en"
        fetched_at = self._hour(max(row["ingested_at"] for row in rows))
        period = f"+{offset}–{offset + limit} h" if en else f"+{offset}–{offset + limit} giờ"
        title = f"WEATHER FORECAST · {label}" if en else f"DỰ BÁO THỜI TIẾT · {label}"
        updated = "Fetched" if en else "Lấy dữ liệu lúc"
        lines = [title, f"Open-Meteo · {updated} {fetched_at} · {period} · {len(rows)}/{limit}"]

        for row in rows:
            timestamp = row["forecast_time"].astimezone(self.bkk_tz).strftime("%H:%M %d/%m")
            temp = f'{row["temperature_2m"]:.1f} °C' if row["temperature_2m"] is not None else "N/A"
            rain = row["precipitation"]
            prob = row["precipitation_probability"]
            if rain is None or prob is None:
                rain_text = "Rain N/A" if en else "Mưa N/A"
            else:
                label = "Rain" if en else "Mưa"
                rain_text = f"{label} {rain:.1f} mm · {prob}%"
                if rain >= self.rain_mm_threshold and prob >= self.rain_prob_threshold:
                    rain_text += " · Alert" if en else " · Cảnh báo"
            wind = f'{row["wind_speed_10m"]:.1f} km/h' if row["wind_speed_10m"] is not None else "N/A"
            uv = f'{row["weather_uv_index"]:.1f}' if row["weather_uv_index"] is not None else "N/A"
            wind_label = "Wind" if en else "Gió"
            lines.append(f"{timestamp} · {temp} · {rain_text} · {wind_label} {wind} · UV {uv}")
        return "\n".join(lines)

    def fmt_aqi(self, label: str, row: dict, lang: str) -> str:
        en = lang == "en"
        title = f"AIR QUALITY FORECAST · {label}" if en else f"DỰ BÁO CHẤT LƯỢNG KHÔNG KHÍ · {label}"
        source = "Model forecast: Open-Meteo / CAMS" if en else "Nguồn dự báo mô hình: Open-Meteo / CAMS"
        if row is None or row["pm2_5"] is None:
            return f'{title}\n{source}\n' + ("No PM2.5 forecast available." if en else "Chưa có dự báo PM2.5.")
        valid_at = self._hour(row["forecast_time"])
        valid = f"Forecast for {valid_at}" if en else f"Dự báo lúc {valid_at}"
        if not row["fresh"]:
            stale = "Forecast is stale." if en else "Dữ liệu đã cũ."
            return f"{title}\n{source}\n{valid}\n{stale}"

        fetched_at = self._hour(row["ingested_at"])
        fetched = f"Fetched {fetched_at}" if en else f"Lấy dữ liệu lúc {fetched_at}"
        pm25 = row["pm2_5"]
        lines = [title, source, f"{valid} · {fetched}", f"PM2.5: {pm25:.1f} µg/m³"]
        for key, name in (("pm10", "PM10"), ("nitrogen_dioxide", "NO₂"), ("ozone", "O₃")):
            if row[key] is not None:
                lines.append(f"{name}: {row[key]:.1f} µg/m³")
        if pm25 >= self.pm25_alert_threshold:
            lines.append("PM2.5 is above the alert threshold." if en else "PM2.5 đạt ngưỡng cảnh báo.")
        return "\n".join(lines)

    def get_guide_text(self, lang: str, name: str) -> str:
        if lang == "en":
            return (
                f"Hi {name}!\n"
                "==========================================\n"
                "WEATHER & AIR QUALITY FORECAST BOT\n"
                "==========================================\n\n"
                "Automated weather and air-quality forecasts.\n"
                "Powered by an ELT Pipeline (Airflow, dbt, PostgreSQL).\n"
                "Data sources: Open-Meteo API & CAMS.\n"
                "Developed by: gkinhere.\n\n"
                "COMMANDS\n"
                "  /weather   Weather forecast\n"
                "  /aqi       PM2.5 forecast\n"
                "  /briefing  Saved location’s 24-hour forecast\n"
                "  /location  Save a location\n"
                "  /subscribe · /unsubscribe  Daily briefing\n"
                "  /language  Change language\n\n"
                "Select a feature to get started:"
            )
        else:
            return (
                f"Chào {name}!\n"
                "==========================================\n"
                "BOT DỰ BÁO THỜI TIẾT & CHẤT LƯỢNG KHÔNG KHÍ\n"
                "==========================================\n\n"
                "Hệ thống cung cấp dữ liệu dự báo thời tiết và chất lượng không khí tự động.\n"
                "Vận hành qua kiến trúc ELT Pipeline (Airflow, dbt, PostgreSQL).\n"
                "Nguồn dữ liệu: Open-Meteo API & Mô hình CAMS.\n"
                "Phát triển bởi: gkinhere.\n\n"
                "CÁC LỆNH\n"
                "  /weather   Dự báo thời tiết\n"
                "  /aqi       Chất lượng không khí\n"
                "  /briefing  Bản tin 24 giờ cho khu vực đã lưu\n"
                "  /location  Lưu khu vực\n"
                "  /subscribe · /unsubscribe  Bản tin mỗi sáng\n"
                "  /language  Đổi ngôn ngữ\n\n"
                "Chọn chức năng bên dưới để bắt đầu:"
            )

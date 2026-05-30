import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
import psycopg2.extras
from datetime import datetime
from zoneinfo import ZoneInfo
from src.utils.logger import get_logger

logger = get_logger("BotServices")

_TEMP_LEVEL_EN = {
    "Nguy hiểm": "Dangerous",
    "Rất nóng":  "Very Hot",
    "Nóng":      "Hot",
    "Dễ chịu":   "Comfortable",
    "Mát mẻ":    "Cool",
    "Chưa có dữ liệu": "N/A",
}
_UV_LEVEL_EN = {
    "Cực kỳ nguy hiểm": "Extreme",
    "Rất cao":           "Very High",
    "Cao":               "High",
    "Trung bình":        "Moderate",
    "Thấp":              "Low",
    "Chưa có dữ liệu":   "N/A",
}
_PM25_LEVEL_EN = {
    "Nguy hiểm":            "Hazardous",
    "Rất không lành mạnh": "Very Unhealthy",
    "Không lành mạnh":     "Unhealthy",
    "Trung bình":           "Moderate",
    "Tốt":                  "Good",
    "Chưa có dữ liệu":      "N/A",
}

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
                port=self.loader.db_port
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

    def query_weather(self, db_name: str, limit: int = 6, offset: int = 0) -> list:
        try:
            sql = """
                SELECT
                    forecast_time,
                    temperature_2m,
                    apparent_temperature,
                    precipitation_probability,
                    precipitation,
                    wind_speed_10m,
                    wind_gusts_10m,
                    cloud_cover,
                    weather_uv_index,
                    uv_level,
                    temperature_level
                FROM gold_layer.mart_hourly_conditions
                WHERE location_name = %s
                  AND forecast_time >= NOW() + CAST(%s AS INTERVAL)
                ORDER BY forecast_time ASC
                LIMIT %s;
            """
            with self.get_db_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    interval_str = f"{offset} hours" if offset > 0 else "-1 hours"
                    cur.execute(sql, (db_name, interval_str, limit))
                    rows = cur.fetchall()
            
            # Loại bỏ cơ chế slicing (rows[::2], rows[::4]) theo yêu cầu
            # để hiển thị đầy đủ chi tiết từng giờ.
            return rows
        except Exception as e:
            logger.error(f"Error query_weather: {e}")
            return []

    def query_aqi(self, db_name: str) -> dict | None:
        try:
            sql = """
                SELECT
                    forecast_time,
                    pm2_5,
                    pm10,
                    pm2_5_level,
                    nitrogen_dioxide,
                    ozone,
                    is_air_quality_alert
                FROM gold_layer.mart_hourly_conditions
                WHERE location_name = %s
                  AND pm2_5 IS NOT NULL
                  AND forecast_time <= NOW() + INTERVAL '1 hour'
                ORDER BY forecast_time DESC
                LIMIT 1;
            """
            with self.get_db_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(sql, (db_name,))
                    row = cur.fetchone()
                    return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error query_aqi: {e}")
            return None


class BotFormatter:
    """Handles text formatting and insight generation for the Telegram Bot."""
    def __init__(self, rain_prob_threshold, rain_mm_threshold, pm25_alert_threshold, bkk_tz):
        self.rain_prob_threshold = rain_prob_threshold
        self.rain_mm_threshold = rain_mm_threshold
        self.pm25_alert_threshold = pm25_alert_threshold
        self.bkk_tz = bkk_tz

    def fmt_weather(self, label: str, rows: list, lang: str, limit: int = 6, offset: int = 0) -> str:
        now = datetime.now(self.bkk_tz).strftime("%H:%M  %d/%m/%Y")
        sep = "─" * 32
        
        time_desc_en = f"Next {limit} hours"
        time_desc_vi = f"{limit} giờ tới"
        if offset > 0:
            time_desc_en = f"Hours +{offset} to +{offset+limit}"
            time_desc_vi = f"Dự báo từ giờ thứ {offset} đến {offset+limit}"
        
        title = f"WEATHER FORECAST · {label.upper()}\nUpdated {now}  |  {time_desc_en}\n{sep}" if lang == "en" else \
                f"DỰ BÁO THỜI TIẾT · {label.upper()}\nCập nhật {now}  |  {time_desc_vi}\n{sep}"
        lines = [title]


        for r in rows:
            ft = r["forecast_time"]
            if ft.tzinfo is not None:
                ft = ft.astimezone(self.bkk_tz)
            t_str = ft.strftime("%H:%M (%d/%m)")
            temp  = float(r["temperature_2m"])
            feels = float(r["apparent_temperature"]) if r["apparent_temperature"] is not None else temp
            prob  = int(r["precipitation_probability"])
            rain  = float(r["precipitation"])
            wind  = float(r["wind_speed_10m"]) if r["wind_speed_10m"] is not None else 0.0
            uv    = float(r["weather_uv_index"]) if r["weather_uv_index"] is not None else 0.0
            cloud = int(r["cloud_cover"]) if r["cloud_cover"] is not None else 0

            if lang == "en":
                tmp_lv = _TEMP_LEVEL_EN.get(r["temperature_level"] or "", r["temperature_level"] or "N/A")
                uv_lv  = _UV_LEVEL_EN.get(r["uv_level"] or "", r["uv_level"] or "N/A")
                
                if rain > 0:
                    if rain >= self.rain_mm_threshold and prob >= self.rain_prob_threshold:
                        rain_desc, alert = "Heavy rain / Flood risk", " [!! ALERT !!]"
                        rain_icon = "⛈️"
                    elif rain >= self.rain_mm_threshold:
                        rain_desc, alert = "Widespread rain", ""
                        rain_icon = "🌧️"
                    else:
                        rain_desc, alert = "Local showers / Thunderstorms", ""
                        rain_icon = "🌦️"
                    rain_text = f"{rain_icon} {rain_desc} ({rain:.1f} mm) | Chance of rain: {prob}%{alert}"
                else:
                    rain_text = "🌤️ No rain expected"
                
                lines += [
                    f"\n{t_str}",
                    f"  Temp : {temp:.1f}C (Feels {feels:.1f}C) [{tmp_lv}]",
                    f"  Rain : {rain_text}",
                    f"  Misc : Wind {wind:.1f} km/h | Cloud {cloud}% | UV {uv:.1f} ({uv_lv})"
                ]
            else:
                tmp_lv = r["temperature_level"] or "N/A"
                uv_lv  = r["uv_level"] or "N/A"

                if rain > 0:
                    if rain >= self.rain_mm_threshold and prob >= self.rain_prob_threshold:
                        rain_desc, alert = "Mưa rất lớn / Nguy cơ ngập", " [!! CẢNH BÁO !!]"
                        rain_icon = "⛈️"
                    elif rain >= self.rain_mm_threshold:
                        rain_desc, alert = "Mưa diện rộng", ""
                        rain_icon = "🌧️"
                    else:
                        rain_desc, alert = "Mưa rào / Dông cục bộ", ""
                        rain_icon = "🌦️"
                    rain_text = f"{rain_icon} {rain_desc} ({rain:.1f} mm) | Tỉ lệ có mưa: {prob}%{alert}"
                else:
                    rain_text = "🌤️ Không mưa"
                
                lines += [
                    f"\n{t_str}",
                    f"  Nhiệt độ: {temp:.1f}C (Cảm giác {feels:.1f}C) [{tmp_lv}]",
                    f"  Mưa     : {rain_text}",
                    f"  Khác    : Gió {wind:.1f} km/h | Mây {cloud}% | UV {uv:.1f} ({uv_lv})"
                ]
                
        lines.append(f"\n{sep}")
        if lang == "en":
            lines.append(f"* Heavy Rain Alert triggers when Volume > {self.rain_mm_threshold:.1f} mm and Chance of rain >= {self.rain_prob_threshold}%")
        else:
            lines.append(f"* Cảnh báo Mưa lớn kích hoạt khi Lượng mưa > {self.rain_mm_threshold:.1f} mm và Tỉ lệ có mưa >= {self.rain_prob_threshold}%")
            
        return "\n".join(lines)

    def fmt_aqi(self, label: str, row: dict, lang: str) -> str:
        now      = datetime.now(self.bkk_tz).strftime("%H:%M  %d/%m/%Y")
        ft = row["forecast_time"]
        if ft.tzinfo is not None:
            ft = ft.astimezone(self.bkk_tz)
        obs_time = ft.strftime("%H:%M (%d/%m)")
        pm25     = float(row["pm2_5"])
        pm10     = float(row["pm10"])             if row["pm10"]             is not None else None
        no2      = float(row["nitrogen_dioxide"]) if row["nitrogen_dioxide"] is not None else None
        o3       = float(row["ozone"])            if row["ozone"]            is not None else None
        is_alert = row["is_air_quality_alert"]

        if lang == "en":
            level = _PM25_LEVEL_EN.get(row["pm2_5_level"] or "", row["pm2_5_level"] or "N/A")
            if is_alert is None:
                status, advice = "[No forecast data]", "AQI data is not yet available."
            elif is_alert:
                status, advice = "[!! AIR QUALITY ALERT !!]", "Limit outdoor activities. Wear an N95 mask."
            else:
                status, advice = "[Safe]", "Air quality is within safe limits for outdoor activities."
                
            lines = [
                f"AIR QUALITY INDEX · {label.upper()}",
                f"Data as of {obs_time} | Updated {now}",
                "─" * 34,
                f"PM2.5   : {pm25:.1f} µg/m³  (Alert >= {self.pm25_alert_threshold:.0f})",
            ]
            if pm10 is not None: lines.append(f"PM10    : {pm10:.1f} µg/m³")
            if no2 is not None:  lines.append(f"NO2     : {no2:.1f} µg/m³")
            if o3 is not None:   lines.append(f"Ozone   : {o3:.1f} µg/m³")
            lines += [
                f"Level   : {level}",
                f"Status  : {status}",
                f"Advice  : {advice}",
            ]
        else:
            level = row["pm2_5_level"] or "N/A"
            if is_alert is None:
                status, advice = "[Chưa có dữ liệu]", "Dữ liệu AQI chưa sẵn sàng."
            elif is_alert:
                status, advice = "[!! CẢNH BÁO Ô NHIỄM !!]", "Hạn chế ra ngoài. Rất nên đeo khẩu trang N95."
            else:
                status, advice = "[An toàn]", "Chất lượng không khí tốt, yên tâm sinh hoạt ngoài trời."
                
            lines = [
                f"CHẤT LƯỢNG KHÔNG KHÍ · {label.upper()}",
                f"Dữ liệu lúc {obs_time} | Cập nhật {now}",
                "─" * 34,
                f"PM2.5   : {pm25:.1f} µg/m³  (Cảnh báo >= {self.pm25_alert_threshold:.0f})",
            ]
            if pm10 is not None: lines.append(f"PM10    : {pm10:.1f} µg/m³")
            if no2 is not None:  lines.append(f"NO2     : {no2:.1f} µg/m³")
            if o3 is not None:   lines.append(f"Ozone   : {o3:.1f} µg/m³")
            lines += [
                f"Mức độ  : {level}",
                f"Trạng thái: {status}",
                f"Khuyến nghị: {advice}",
            ]
            
        return "\n".join(lines)

    def get_guide_text(self, lang: str, name: str) -> str:
        if lang == "en":
            return (
                f"Hi {name}!\n"
                "==========================================\n"
                "WEATHER & AIR QUALITY FORECAST BOT\n"
                "==========================================\n\n"
                "Automated Weather & AQI Data System.\n"
                "Powered by an ELT Pipeline (Airflow, dbt, PostgreSQL).\n"
                "Data sources: Open-Meteo API & CAMS.\n"
                "Developed by: gkinhere.\n\n"
                "COMMANDS\n"
                "  /weather   Weather forecast\n"
                "  /aqi       Air quality index\n"
                "  /start     Show this guide & language settings\n\n"
                "Select a feature to get started:"
            )
        else:
            return (
                f"Chào {name}!\n"
                "==========================================\n"
                "BOT DỰ BÁO THỜI TIẾT & CHẤT LƯỢNG KHÔNG KHÍ\n"
                "==========================================\n\n"
                "Hệ thống cung cấp dữ liệu Thời Tiết & AQI tự động.\n"
                "Vận hành qua kiến trúc ELT Pipeline (Airflow, dbt, PostgreSQL).\n"
                "Nguồn dữ liệu: Open-Meteo API & Mô hình CAMS.\n"
                "Phát triển bởi: gkinhere.\n\n"
                "CÁC LỆNH\n"
                "  /weather   Dự báo thời tiết\n"
                "  /aqi       Chất lượng không khí\n"
                "  /start     Đổi ngôn ngữ & xem hướng dẫn\n\n"
                "Chọn chức năng bên dưới để bắt đầu:"
            )

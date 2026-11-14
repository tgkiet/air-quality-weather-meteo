"""
alert_job.py - Lõi Push của Dual-Core Bot Architecture.
Phát thanh 3 bản tin:
1. Kế hoạch Ngày Mai (20:00)
2. Bảo vệ Phổi (06:00)
3. Cảnh báo Khẩn cấp (Các giờ còn lại - Stateful deduplication)
"""

import sys
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import defaultdict
import argparse
import re
from html import escape

# Ensure the src module is discoverable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.loaders.base_loader import BasePostgresLoader
from src.utils.telegram_alerter import TelegramAlerter, TelegramDeliveryError
from src.utils.logger import get_logger
from src.utils.config_manager import config_manager
from psycopg2.extras import RealDictCursor, execute_values
from src.scripts.bot_services import (
    AQ_BATCH_JOIN, AQ_FRESH, BotDatabaseManager, BotFormatter, FRESHNESS_HOURS,
    WEATHER_BATCH_JOIN, WEATHER_FRESH,
)

logger = get_logger("AlertJob")

class WeatherAlerterJob(BasePostgresLoader):
    """
    Job quét dữ liệu Data Mart để gửi cảnh báo định kỳ và khẩn cấp.
    Được cấu trúc theo nguyên lý Lập trình Hướng Đối Tượng (OOP).
    Không chứa hardcode icon emoji, đảm bảo format production sạch sẽ.
    """
    def __init__(self, execution_date: datetime):
        super().__init__()
        self.alerter = TelegramAlerter()
        self.exec_date = execution_date
        try:
            self.scheduled_region_prefix = config_manager.alert_job_config["scheduled_region_prefix"]
            self.sudden_region_prefix = config_manager.alert_job_config["sudden_region_prefix"]
        except KeyError as e:
            raise ValueError(f"Missing region prefix in config: {e}")
        self.bkk_tz = ZoneInfo('Asia/Bangkok')

    def _delivery_id(self, kind, chat_id=None):
        return f"{kind}:{chat_id or self.alerter.chat_id}:{self.exec_date.timestamp():.0f}"

    def _send_once(self, kind, message, *, chat_id=None, rain_keys=(), parse_mode="HTML"):
        delivery_id = self._delivery_id(kind, chat_id)
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('silver_layer.telegram_delivery')")
            if cursor.fetchone()[0] is None:
                # Existing instances can send while the optional delivery migration is pending.
                self.alerter.send_message(message, chat_id=chat_id, parse_mode=parse_mode)
                if rain_keys:
                    self._reserve_rain(cursor, rain_keys)
                self.connection.commit()
                return

            cursor.execute("""
                INSERT INTO silver_layer.telegram_delivery (delivery_id)
                VALUES (%s) ON CONFLICT DO NOTHING RETURNING delivery_id
            """, (delivery_id,))
            if cursor.fetchone() is None:
                cursor.execute("SELECT delivered_at FROM silver_layer.telegram_delivery WHERE delivery_id = %s",
                               (delivery_id,))
                delivered = cursor.fetchone()[0] is not None
                self.connection.commit()
                if delivered:
                    return
                raise TelegramDeliveryError("Previous Telegram delivery status unknown; review before retrying")
            if rain_keys:
                execute_values(cursor, """
                    INSERT INTO silver_layer.alert_history (location_name, forecast_time, alert_type, delivery_id)
                    VALUES %s ON CONFLICT DO NOTHING
                """, [(*key, delivery_id) for key in rain_keys])
        # Commit before contacting Telegram. A read timeout or process crash can
        # occur after delivery, so an unacknowledged claim always needs review.
        self.connection.commit()
        try:
            self.alerter.send_message(message, chat_id=chat_id, parse_mode=parse_mode)
        except TelegramDeliveryError as error:
            if error.retryable:
                with self.connection.cursor() as cursor:
                    if rain_keys:
                        cursor.execute("DELETE FROM silver_layer.alert_history WHERE delivery_id = %s",
                                       (delivery_id,))
                    cursor.execute("DELETE FROM silver_layer.telegram_delivery WHERE delivery_id = %s",
                                   (delivery_id,))
                self.connection.commit()
            raise
        with self.connection.cursor() as cursor:
            cursor.execute("""
                UPDATE silver_layer.telegram_delivery SET delivered_at = NOW() WHERE delivery_id = %s
            """, (delivery_id,))
        self.connection.commit()

    @staticmethod
    def _reserve_rain(cursor, keys):
        execute_values(cursor, """
            INSERT INTO silver_layer.alert_history (location_name, forecast_time, alert_type)
            VALUES %s ON CONFLICT DO NOTHING
        """, keys)

    @staticmethod
    def _short_message(message):
        if len(message) <= 4000:
            return message
        txt = re.sub(r"<[^>]+>", "", message)
        return txt[:3800] + "\n… Mở /weather để xem chi tiết.\nNguồn dự báo: Open-Meteo / CAMS."

    def _send_rain_alert(self, records, title):
        by_location = defaultdict(list)
        for row in records:
            by_location[row["location_name"]].append(row)
        lines = [f"<b>{escape(title)}</b>"]
        sent_keys = []
        for location, rows in by_location.items():
            peak = max(rows, key=lambda row: row["precipitation"])
            peak_time = peak["forecast_time"].astimezone(self.bkk_tz).strftime("%H:%M %d/%m")
            line = (f"{escape(location)} · {peak_time} · {peak['precipitation']:.1f} mm"
                    f" · {peak['precipitation_probability']}%")
            if sum(len(item) + 1 for item in lines) + len(line) > 3800:
                break
            lines.append(line)
            sent_keys.extend((row["location_name"], row["forecast_time"], "RAIN") for row in rows)
        if sent_keys:
            self._send_once("rain", "\n".join(lines + ["Nguồn dự báo: Open-Meteo."]), rain_keys=sent_keys)

    def run_holistic_briefing(self, is_morning=True, start_hour=6):
        window_name = "HÔM NAY" if is_morning else "NGÀY MAI"
        logger.info(f"Running Holistic Briefing for {window_name}...")
        
        try:
            thresholds = config_manager.alert_thresholds
            rain_mm = float(thresholds["rain_mm"])
            rain_prob = int(thresholds["rain_probability_pct"])
            uv_alert = float(thresholds.get("uv_alert_index", 10.0))
            heat_alert = float(thresholds.get("heatwave_alert_temp", 38.0))
        except KeyError as e:
            raise ValueError(f"Missing thresholds in config: {e}")
            
        start_interval = f"{start_hour} hours" if is_morning else "1 day"
        end_interval = "1 day" if is_morning else "2 days"
        
        query = f"""
            SELECT g.location_name, g.forecast_time,
                   g.precipitation_probability, g.precipitation,
                   CASE WHEN {AQ_FRESH} THEN aq.pm2_5 END AS pm2_5,
                   COALESCE({AQ_FRESH} AND aq.pm2_5 >= %s, FALSE) AS is_air_quality_alert,
                   g.weather_uv_index, g.apparent_temperature
            FROM gold_layer.mart_hourly_conditions AS g
            {WEATHER_BATCH_JOIN}
            {AQ_BATCH_JOIN}
            WHERE {WEATHER_FRESH}
              AND g.forecast_time >= (((%s::TIMESTAMPTZ AT TIME ZONE 'Asia/Bangkok')::DATE + CAST(%s AS INTERVAL)) AT TIME ZONE 'Asia/Bangkok')
              AND g.forecast_time < (((%s::TIMESTAMPTZ AT TIME ZONE 'Asia/Bangkok')::DATE + CAST(%s AS INTERVAL)) AT TIME ZONE 'Asia/Bangkok')
              AND g.location_name LIKE %s
            ORDER BY g.location_name ASC, g.forecast_time ASC;
        """
        prefix_val = self.scheduled_region_prefix.strip()
        prefix = prefix_val + ' %' if prefix_val else '%'
        
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (
                float(thresholds["pm25_alert_ugm3"]), self.exec_date.isoformat(), start_interval,
                self.exec_date.isoformat(), end_interval, prefix,
            ))
            records = cursor.fetchall()
            
        title_region = f" ({prefix_val})" if prefix_val else ""
        title = escape(f"BẢN TIN THỜI TIẾT {window_name}{title_region}")
        kind = "morning" if is_morning else "evening"
            
        if not records:
            self._send_once(kind, f"<b>{title}</b>\nChưa đủ dữ liệu dự báo mới cho khoảng thời gian này.")
            return

        grouped_records = defaultdict(list)
        for record in records:
            grouped_records[record["location_name"]].append(record)

        alerts = defaultdict(list)
        for loc, loc_records in sorted(grouped_records.items()):
            rain_records = [
                row for row in loc_records if row["precipitation"] is not None
                and row["precipitation_probability"] is not None
                and row["precipitation"] >= rain_mm and row["precipitation_probability"] >= rain_prob
            ]
            if rain_records:
                start_time = rain_records[0]['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                end_time = rain_records[-1]['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                t_str = f" ({start_time}-{end_time})" if start_time != end_time else f" ({start_time})"
                
                max_rain_record = max(rain_records, key=lambda x: float(x['precipitation']))
                max_rain = float(max_rain_record['precipitation'])
                max_prob = max_rain_record['precipitation_probability']
                peak_time = max_rain_record['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                
                rain_type = "Mưa lớn" if max_rain >= 5.0 else "Mưa vừa"
                alerts[loc].append(f"[Mưa] {rain_type}{t_str}: To nhất lúc {peak_time} ({max_rain:.1f}mm, Tỉ lệ {max_prob}%)")
                
            # PM2.5
            aqi_records = [r for r in loc_records if r['is_air_quality_alert']]
            if aqi_records:
                start_time = aqi_records[0]['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                end_time = aqi_records[-1]['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                t_str = f" ({start_time}-{end_time})" if start_time != end_time else f" ({start_time})"
                
                max_aq_record = max(aqi_records, key=lambda x: float(x['pm2_5']))
                max_pm25 = float(max_aq_record['pm2_5'])
                peak_time = max_aq_record['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                
                alerts[loc].append(f"[Bụi mịn]{t_str}: Đỉnh điểm lúc {peak_time} (PM2.5: {max_pm25:.1f} µg/m³)")
                
            # UV & Heat
            heat_records = [r for r in loc_records if (r['apparent_temperature'] is not None and r['apparent_temperature'] >= heat_alert) or (r['weather_uv_index'] is not None and r['weather_uv_index'] >= uv_alert)]
            if heat_records:
                start_time = heat_records[0]['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                end_time = heat_records[-1]['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                t_str = f" ({start_time}-{end_time})" if start_time != end_time else f" ({start_time})"
                
                max_temp_record = max([r for r in heat_records if r['apparent_temperature'] is not None], key=lambda x: float(x['apparent_temperature']), default=None)
                max_uv_record = max([r for r in heat_records if r['weather_uv_index'] is not None], key=lambda x: float(x['weather_uv_index']), default=None)
                
                insight_parts = []
                if max_temp_record and float(max_temp_record['apparent_temperature']) >= heat_alert:
                    t_peak = max_temp_record['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                    insight_parts.append(f"Đỉnh nóng {t_peak} (Cảm giác {float(max_temp_record['apparent_temperature']):.1f}°C)")
                
                if max_uv_record and float(max_uv_record['weather_uv_index']) >= uv_alert:
                    uv_peak = max_uv_record['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                    insight_parts.append(f"UV cực đại {uv_peak} ({float(max_uv_record['weather_uv_index']):.1f})")
                
                alerts[loc].append(f"[Nắng gắt]{t_str}: " + ", ".join(insight_parts))

        source = "Nguồn dự báo mô hình: Open-Meteo / CAMS."
        if not alerts:
            self._send_once(
                kind,
                f"<b>{title}</b>\nKhông ghi nhận mức cảnh báo trong dữ liệu hiện có.\n{source}",
            )
            return

        message_lines = [f"<b>{title}</b>", "============================="]
        
        # Generate Global Insight
        has_rain = any("[Mưa]" in a for loc_alerts in alerts.values() for a in loc_alerts)
        has_aqi = any("[Bụi mịn]" in a for loc_alerts in alerts.values() for a in loc_alerts)
        has_heat = any("[Nắng gắt]" in a for loc_alerts in alerts.values() for a in loc_alerts)
        
        insights = []
        if has_rain: insights.append("mang theo đồ che mưa")
        if has_aqi: insights.append("đeo khẩu trang N95")
        if has_heat: insights.append("hạn chế ra đường giữa trưa nắng gắt")
        
        if insights:
            insight_msg = f"<b>LỜI KHUYÊN:</b> Hôm nay hãy nhớ " + ", ".join(insights) + " nhé!"
            message_lines.append(insight_msg)
            message_lines.append("=============================")

        for loc, loc_alerts in alerts.items():
            if not loc_alerts: continue
            short_loc = loc.replace("Thành phố ", "TP.").replace("Thị xã ", "TX.").replace("Quận ", "Q.").replace("Huyện ", "H.")
            message_lines.append(f"<b>{escape(short_loc)}</b>:")
            for a in loc_alerts:
                message_lines.append(f" - {a}")
        
        message_lines.append("=============================")
        message_lines.append("<i>Sử dụng /menu với Bot để xem chi tiết.</i>")
        
        message_lines.append(source)
        final_message = "\n".join(message_lines)
        parse_mode = "HTML" if len(final_message) <= 4000 else None
        self._send_once(kind, self._short_message(final_message), parse_mode=parse_mode)

    def send_personal_briefings(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('silver_layer.telegram_delivery')")
            if cursor.fetchone()[0] is None:
                return
            cursor.execute("""
                SELECT chat_id, home_location, language
                FROM silver_layer.bot_user_preferences
                WHERE daily_briefing AND home_location IS NOT NULL
                ORDER BY chat_id
            """)
            subscribers = cursor.fetchall()
        if not subscribers:
            return

        thresholds = config_manager.alert_thresholds
        formatter = BotFormatter(
            thresholds["rain_probability_pct"], thresholds["rain_mm"],
            thresholds["pm25_alert_ugm3"], self.bkk_tz,
        )
        db = BotDatabaseManager(self)
        locations = {item["name"] for item in config_manager.locations}
        forecasts = {}
        failures = 0
        for chat, location, language in subscribers:
            if location not in locations:
                continue
            if location not in forecasts:
                forecasts[location] = db.query_briefing(location)
            message = formatter.fmt_briefing(location, forecasts[location], language)
            try:
                self._send_once("daily", message, chat_id=chat, parse_mode=None)
            except TelegramDeliveryError:
                failures += 1
        if failures:
            raise TelegramDeliveryError(f"Daily briefing failed for {failures} recipient(s)")

    def run_sudden_alert(self):
        """Cảnh báo Đột xuất - Chạy các khung giờ còn lại. Chỉ nhìn 6H tới. Stateful chống Spam."""
        logger.info("Running Sudden Alert Check (6h Window)...")
        try:
            rain_mm = float(config_manager.alert_thresholds["rain_mm"])
            rain_prob = int(config_manager.alert_thresholds["rain_probability_pct"])
        except KeyError as e:
            raise ValueError(f"Missing rain thresholds in config: {e}")
        
        query = f"""
            SELECT g.location_name, g.forecast_time, g.precipitation_probability, g.precipitation
            FROM gold_layer.mart_hourly_conditions g
            {WEATHER_BATCH_JOIN}
            LEFT JOIN silver_layer.alert_history ah 
                ON g.location_name = ah.location_name 
                AND g.forecast_time = ah.forecast_time 
                AND ah.alert_type = 'RAIN'
            WHERE {WEATHER_FRESH}
              AND g.forecast_time >= now()
              AND g.forecast_time < now() + INTERVAL '6 hours'
              AND g.precipitation >= %s 
              AND g.precipitation_probability >= %s
              AND g.location_name LIKE %s
              AND ah.id IS NULL
            ORDER BY g.location_name ASC, g.forecast_time ASC;
        """
        prefix_val = self.sudden_region_prefix.strip()
        prefix = prefix_val + ' %' if prefix_val else '%'
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Serialize overlapping claims for this project before reading history.
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext('meteo_rain_alert'))")
            cursor.execute("SELECT to_regclass('silver_layer.telegram_delivery') AS delivery_table")
            if cursor.fetchone()["delivery_table"] is not None:
                cursor.execute("SELECT delivered_at FROM silver_layer.telegram_delivery WHERE delivery_id = %s",
                               (self._delivery_id("rain"),))
                previous = cursor.fetchone()
                if previous is not None:
                    if previous["delivered_at"] is None:
                        raise TelegramDeliveryError("Previous rain alert delivery status unknown; review before retrying")
                    return
            cursor.execute(query, (rain_mm, rain_prob, prefix))
            records = cursor.fetchall()
        
        if records:
            title_region = f" ({prefix_val})" if prefix_val else ""
            title = f"CẢNH BÁO MƯA TRONG 6 GIỜ TỚI{title_region}"
            self._send_rain_alert(records, title)
        else:
            logger.info("No sudden alerts detected in the next 6 hours.")

    def run_alert_check(self):
        self.connect()
        try:
            now_utc = datetime.now(ZoneInfo('UTC'))
            if (now_utc - self.exec_date).total_seconds() > FRESHNESS_HOURS * 3600:
                logger.info("Skipping a run whose logical time is too old for live alerts.")
                return

            try:
                schedule = config_manager.alert_job_config.get("schedule_hours", {"morning": 6, "evening": 20})
                morning_h = schedule["morning"]
                evening_h = schedule["evening"]
            except KeyError:
                morning_h = 6
                evening_h = 20

            current_hour = self.exec_date.astimezone(self.bkk_tz).hour
            logger.info(f"Execution BKK Hour: {current_hour}")
            
            if current_hour == evening_h:
                self.run_holistic_briefing(is_morning=False)
            elif current_hour == morning_h:
                try:
                    self.run_holistic_briefing(is_morning=True, start_hour=morning_h)
                except TelegramDeliveryError:
                    self.connection.rollback()
                    self.send_personal_briefings()
                    raise
                self.send_personal_briefings()
            else:
                self.run_sudden_alert()

            self.connection.commit()
        except Exception as e:
            logger.error(f"Error in alert job: {e}")
            self.connection.rollback()
            raise
        finally:
            self.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather Alert Job")
    parser.add_argument("--execution_date", type=str, required=True, help="Airflow logical date (ISO format)")
    args = parser.parse_args()
    
    exec_dt = datetime.fromisoformat(args.execution_date)
    if exec_dt.tzinfo is None:
        exec_dt = exec_dt.replace(tzinfo=ZoneInfo('UTC'))
        
    logger.info("Starting Weather Alert Job (Dual-Core Architecture)...")
    job = WeatherAlerterJob(execution_date=exec_dt)
    job.run_alert_check()
    logger.info("Alert Job finished.")

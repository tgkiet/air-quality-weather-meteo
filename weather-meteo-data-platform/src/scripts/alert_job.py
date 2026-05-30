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

# Ensure the src module is discoverable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.loaders.base_loader import BasePostgresLoader
from src.utils.telegram_alerter import TelegramAlerter
from src.utils.logger import get_logger
from src.utils.config_manager import config_manager
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2 import Error

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

    def group_and_send_messages(self, records, title, alert_type, is_stateful=False):
        """Hàm dùng chung để gom nhóm thời gian (Time-range Grouping) và gửi tin nhắn Telegram."""
        if not records:
            logger.info(f"[{title}] No records found.")
            return

        # 1. Gom nhóm theo Quận (location_name)
        grouped_data = defaultdict(list)
        for r in records:
            # Rút gọn tên quận cho dễ nhìn trên điện thoại
            loc = r['location_name'].replace("Thành phố ", "TP.").replace("Thị xã ", "TX.").replace("Quận ", "Q.").replace("Huyện ", "H.")
            grouped_data[loc].append(r)

        # Sử dụng thẻ pre và code của HTML để format (loại bỏ emoji)
        message_lines = [f"<b>{title}</b>", "============================="]
        insert_history_records = []

        # 2. Xử lý logic gộp giờ
        for loc, rows in grouped_data.items():
            start_ft = rows[0]['forecast_time']
            end_ft = rows[-1]['forecast_time']
            if start_ft.tzinfo is not None: start_ft = start_ft.astimezone(self.bkk_tz)
            if end_ft.tzinfo is not None: end_ft = end_ft.astimezone(self.bkk_tz)
            start_time = start_ft.strftime("%H:%M")
            end_time = end_ft.strftime("%H:%M")
            time_range = f"{start_time} - {end_time}" if start_time != end_time else f"{start_time}"
            
            if alert_type == "RAIN":
                max_rain_record = max(rows, key=lambda r: float(r['precipitation']))
                max_rain = float(max_rain_record['precipitation'])
                max_prob = max_rain_record['precipitation_probability']
                peak_time = max_rain_record['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                
                rain_type = "Mưa lớn" if max_rain >= 5.0 else "Mưa vừa"
                message_lines.append(f"[Mưa] {loc} ({time_range}) | Đỉnh điểm lúc {peak_time} ({max_rain:.1f} mm, Tỉ lệ: <b>{max_prob}%</b>)")
            elif alert_type == "AQI":
                max_aq_record = max(rows, key=lambda r: float(r['pm2_5']))
                max_pm25 = float(max_aq_record['pm2_5'])
                level = max_aq_record['pm2_5_level']
                peak_time = max_aq_record['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                
                alert_tag = "[CẢNH BÁO]" if "Nguy hiểm" in level or "Không lành mạnh" in level else "[THÔNG TIN]"
                message_lines.append(f"{alert_tag} <b>{loc}</b> | Xấu nhất lúc {peak_time} (PM2.5: {max_pm25:.1f} µg/m³ - {level})")

            # Chuẩn bị dữ liệu ghi lịch sử (nếu là dạng khẩn cấp chống spam)
            if is_stateful:
                for r in rows:
                    insert_history_records.append((r['location_name'], r['forecast_time'], alert_type))

        message_lines.append("=============================")
        message_lines.append("<i>Sử dụng /menu với Bot để xem chi tiết từng Quận.</i>")

        # 3. Chia nhỏ tin nhắn nếu quá dài (Bảo vệ lỗi 4096 chars của Telegram)
        final_message = "\n".join(message_lines)
        if len(final_message) > 4000:
            # Loại bỏ các thẻ HTML an toàn thay vì dùng Regex gây mất dữ liệu
            clean_text = final_message.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
            final_message = clean_text[:4000] + "...\n(Tin nhan qua dai, da bi cat phan cuoi)"

        success = self.alerter.send_message(final_message)

        # 4. Commit State nếu là Alert Khẩn cấp
        if success and is_stateful and insert_history_records:
            insert_query = """
                INSERT INTO silver_layer.alert_history (location_name, forecast_time, alert_type)
                VALUES %s
                ON CONFLICT (location_name, forecast_time, alert_type) DO NOTHING;
            """
            with self.connection.cursor() as cursor:
                execute_values(cursor, insert_query, insert_history_records)
            self.connection.commit()
            logger.info(f"[{title}] Successfully updated alert_history.")
        elif is_stateful and not success:
            self.connection.rollback()
            logger.error(f"[{title}] Failed to send alert. State rollback applied.")

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
        
        query = """
            SELECT location_name, forecast_time, precipitation_probability, precipitation,
                   pm2_5, pm2_5_level, is_air_quality_alert, weather_uv_index, apparent_temperature
            FROM gold_layer.mart_hourly_conditions
            WHERE forecast_time >= (((%s::TIMESTAMPTZ AT TIME ZONE 'Asia/Bangkok')::DATE + CAST(%s AS INTERVAL)) AT TIME ZONE 'Asia/Bangkok')
              AND forecast_time < (((%s::TIMESTAMPTZ AT TIME ZONE 'Asia/Bangkok')::DATE + CAST(%s AS INTERVAL)) AT TIME ZONE 'Asia/Bangkok')
              AND location_name LIKE %s
            ORDER BY location_name ASC, forecast_time ASC;
        """
        prefix_val = self.scheduled_region_prefix.strip()
        prefix = prefix_val + ' %' if prefix_val else '%'
        
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (self.exec_date.isoformat(), start_interval, self.exec_date.isoformat(), end_interval, prefix))
            records = cursor.fetchall()
            
        title_region = f" ({prefix_val})" if prefix_val else ""
        title = f"BẢN TIN THỜI TIẾT {window_name}{title_region}"
            
        if not records:
            logger.info(f"No records found for {window_name}.")
            msg = f"<b>{title}</b>\n=============================\nThời tiết lý tưởng, không có dữ liệu để cảnh báo rủi ro.\nChúc bạn một ngày tuyệt vời!\n=============================\n<i>(Hệ thống tự động)</i>"
            self.alerter.send_message(msg)
            return
            
        alerts = defaultdict(list)
        for loc in sorted(list(set(r['location_name'] for r in records))):
            loc_records = [r for r in records if r['location_name'] == loc]
            
            # Rain
            rain_records = [r for r in loc_records if r['precipitation'] >= rain_mm and r['precipitation_probability'] >= rain_prob]
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
                
            # AQI
            aqi_records = [r for r in loc_records if r['is_air_quality_alert']]
            if aqi_records:
                start_time = aqi_records[0]['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                end_time = aqi_records[-1]['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                t_str = f" ({start_time}-{end_time})" if start_time != end_time else f" ({start_time})"
                
                max_aq_record = max(aqi_records, key=lambda x: float(x['pm2_5']))
                max_pm25 = float(max_aq_record['pm2_5'])
                max_level = max_aq_record['pm2_5_level']
                peak_time = max_aq_record['forecast_time'].astimezone(self.bkk_tz).strftime("%H:%M")
                
                alerts[loc].append(f"[Bụi mịn]{t_str}: Đỉnh điểm lúc {peak_time} (PM2.5: {max_pm25:.1f} - {max_level})")
                
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

        if not alerts:
            msg = f"<b>{title}</b>\n=============================\nThời tiết lý tưởng, không có cảnh báo rủi ro (Mưa lớn/Bụi mịn/Nắng gắt).\nChúc bạn một ngày tuyệt vời!\n=============================\n<i>(Hệ thống tự động)</i>"
            self.alerter.send_message(msg)
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
            message_lines.append(f"<b>{short_loc}</b>:")
            for a in loc_alerts:
                message_lines.append(f" - {a}")
        
        message_lines.append("=============================")
        message_lines.append("<i>Sử dụng /menu với Bot để xem chi tiết.</i>")
        
        final_message = "\n".join(message_lines)
        if len(final_message) > 4000:
            clean_text = final_message.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
            final_message = clean_text[:4000] + "...\n(Tin nhan qua dai, da bi cat phan cuoi)"

        self.alerter.send_message(final_message)

    def run_sudden_alert(self):
        """Cảnh báo Đột xuất - Chạy các khung giờ còn lại. Chỉ nhìn 6H tới. Stateful chống Spam."""
        logger.info("Running Sudden Alert Check (6h Window)...")
        try:
            rain_mm = float(config_manager.alert_thresholds["rain_mm"])
            rain_prob = int(config_manager.alert_thresholds["rain_probability_pct"])
        except KeyError as e:
            raise ValueError(f"Missing rain thresholds in config: {e}")
        
        query = """
            SELECT g.location_name, g.forecast_time, g.precipitation_probability, g.precipitation
            FROM gold_layer.mart_hourly_conditions g
            LEFT JOIN silver_layer.alert_history ah 
                ON g.location_name = ah.location_name 
                AND g.forecast_time = ah.forecast_time 
                AND ah.alert_type = 'RAIN'
            WHERE g.forecast_time >= %s::TIMESTAMPTZ
              AND g.forecast_time <= %s::TIMESTAMPTZ + INTERVAL '6 hours'
              AND g.precipitation >= %s 
              AND g.precipitation_probability >= %s
              AND g.location_name LIKE %s
              AND ah.id IS NULL
            ORDER BY g.location_name ASC, g.forecast_time ASC;
        """
        prefix_val = self.sudden_region_prefix.strip()
        prefix = prefix_val + ' %' if prefix_val else '%'
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (self.exec_date.isoformat(), self.exec_date.isoformat(), rain_mm, rain_prob, prefix))
            records = cursor.fetchall()
        
        if records:
            title_region = f" ({prefix_val})" if prefix_val else ""
            title = f"CẢNH BÁO KHẨN: MƯA LỚN ĐỘT XUẤT TRONG 6H TỚI{title_region}"
            self.group_and_send_messages(records, title, "RAIN", is_stateful=True)
        else:
            logger.info("No sudden alerts detected in the next 6 hours.")

    def run_alert_check(self):
        self.connect()
        try:
            now_utc = datetime.now(ZoneInfo('UTC'))
            if (now_utc - self.exec_date).total_seconds() > 86400:
                logger.info("This is a historical backfill run (> 24 hours old). Skipping Telegram alerts to prevent SPAM.")
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
                self.run_holistic_briefing(is_morning=True, start_hour=morning_h)
            else:
                self.run_sudden_alert()

        except Exception as e:
            logger.error(f"Error in alert job: {e}")
            self.connection.rollback()
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

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
    def __init__(self):
        super().__init__()
        self.alerter = TelegramAlerter()
        try:
            self.target_region_prefix = config_manager.alert_job_config["target_region_prefix"]
        except KeyError as e:
            raise ValueError(f"Missing target_region_prefix in config: {e}")
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
                max_prob = max(r['precipitation_probability'] for r in rows)
                max_rain = max(float(r['precipitation']) for r in rows)
                message_lines.append(f"{loc} ({time_range}) | Mưa lớn ({max_rain:.1f} mm) | Tỉ lệ có mưa: <b>{max_prob}%</b>")
            elif alert_type == "AQI":
                max_pm25 = max(r['pm2_5'] for r in rows)
                level = rows[0]['pm2_5_level']
                alert_tag = "[CẢNH BÁO]" if "Nguy hiểm" in level or "Không lành mạnh" in level else "[THÔNG TIN]"
                message_lines.append(f"{alert_tag} <b>{loc}</b> | PM2.5: {float(max_pm25):.1f} µg/m³ ({level})")

            # Chuẩn bị dữ liệu ghi lịch sử (nếu là dạng khẩn cấp chống spam)
            if is_stateful:
                for r in rows:
                    insert_history_records.append((r['location_name'], r['forecast_time'], alert_type))

        message_lines.append("=============================")
        message_lines.append("<i>Sử dụng /menu với Bot để xem chi tiết từng Quận.</i>")

        # 3. Chia nhỏ tin nhắn nếu quá dài (Bảo vệ lỗi 4096 chars của Telegram)
        final_message = "\n".join(message_lines)
        if len(final_message) > 4000:
            final_message = final_message[:4000] + "...\n(Tin nhan qua dai, da bi cat phan cuoi)"

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

    def run_evening_briefing(self):
        """Bản tin Tối (20:00) - Tổng hợp rủi ro mưa lớn của toàn bộ NGÀY MAI."""
        logger.info("Running Evening Briefing (20:00)...")
        try:
            rain_mm = float(config_manager.alert_thresholds["rain_mm"])
            rain_prob = int(config_manager.alert_thresholds["rain_probability_pct"])
        except KeyError as e:
            raise ValueError(f"Missing rain thresholds in config: {e}")
        
        query = """
            SELECT location_name, forecast_time, precipitation_probability, precipitation
            FROM gold_layer.mart_hourly_conditions
            WHERE forecast_time >= ((NOW() AT TIME ZONE 'Asia/Bangkok')::DATE + INTERVAL '1 day') AT TIME ZONE 'Asia/Bangkok'
              AND forecast_time < ((NOW() AT TIME ZONE 'Asia/Bangkok')::DATE + INTERVAL '2 days') AT TIME ZONE 'Asia/Bangkok'
              AND precipitation > %s 
              AND precipitation_probability >= %s
              AND location_name LIKE %s
            ORDER BY location_name ASC, forecast_time ASC;
        """
        prefix = self.target_region_prefix.strip() + ' %'
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (rain_mm, rain_prob, prefix))
            records = cursor.fetchall()
        
        title = f"BẢN TIN MƯA LỚN NGÀY MAI ({self.target_region_prefix.strip()})"
        self.group_and_send_messages(records, title, "RAIN", is_stateful=False)

    def run_morning_aqi_briefing(self):
        """Bản tin Sáng (06:00) - Khuyến cáo AQI trong 24h tới để dân tình chuẩn bị khẩu trang."""
        logger.info("Running Morning AQI Briefing (06:00)...")
        query = """
            SELECT location_name, forecast_time, pm2_5, pm2_5_level
            FROM gold_layer.mart_hourly_conditions
            WHERE forecast_time >= ((NOW() AT TIME ZONE 'Asia/Bangkok')::DATE + INTERVAL '6 hours') AT TIME ZONE 'Asia/Bangkok'
              AND forecast_time < ((NOW() AT TIME ZONE 'Asia/Bangkok')::DATE + INTERVAL '1 day') AT TIME ZONE 'Asia/Bangkok'
              AND is_air_quality_alert = TRUE
              AND location_name LIKE %s
            ORDER BY location_name ASC, forecast_time ASC;
        """
        prefix = self.target_region_prefix.strip() + ' %'
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (prefix,))
            records = cursor.fetchall()
        
        title = f"CẢNH BÁO BỤI MỊN PM2.5 SÁNG NAY ({self.target_region_prefix.strip()})"
        self.group_and_send_messages(records, title, "AQI", is_stateful=False)

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
            WHERE g.forecast_time >= NOW()
              AND g.forecast_time <= NOW() + INTERVAL '6 hours'
              AND g.precipitation > %s 
              AND g.precipitation_probability >= %s
              AND g.location_name LIKE %s
              AND ah.id IS NULL
            ORDER BY g.location_name ASC, g.forecast_time ASC;
        """
        prefix = self.target_region_prefix.strip() + ' %'
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (rain_mm, rain_prob, prefix))
            records = cursor.fetchall()
        
        if records:
            title = f"CẢNH BÁO KHẨN: MƯA LỚN ĐỘT XUẤT TRONG 6H TỚI"
            self.group_and_send_messages(records, title, "RAIN", is_stateful=True)
        else:
            logger.info("No sudden alerts detected in the next 6 hours.")

    def run_alert_check(self):
        self.connect()
        try:
            current_hour = datetime.now(self.bkk_tz).hour
            logger.info(f"Current BKK Hour: {current_hour}")
            
            if current_hour == 20:
                self.run_evening_briefing()
            elif current_hour == 6:
                self.run_morning_aqi_briefing()
            else:
                self.run_sudden_alert()

        except Exception as e:
            logger.error(f"Error in alert job: {e}")
            self.connection.rollback()
        finally:
            self.close()

if __name__ == "__main__":
    logger.info("Starting Weather Alert Job (Dual-Core Architecture)...")
    job = WeatherAlerterJob()
    job.run_alert_check()
    logger.info("Alert Job finished.")

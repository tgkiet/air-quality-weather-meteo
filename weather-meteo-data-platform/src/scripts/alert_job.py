"""
alert_job.py - Script độc lập để quét dữ liệu cảnh báo từ tầng Gold và gửi Telegram.
Kiến trúc: Stateful Push Alerting (Chống Spam).
Kế thừa BasePostgresLoader để sử dụng connection pool an toàn.
"""

import sys
import os

# Ensure the src module is discoverable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.loaders.base_loader import BasePostgresLoader
from src.utils.telegram_alerter import TelegramAlerter
from src.utils.logger import get_logger
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2 import Error

logger = get_logger("AlertJob")

class WeatherAlerterJob(BasePostgresLoader):
    """
    Job kiểm tra cảnh báo nguy cơ mưa lớn (Xác suất > 80% và Lượng mưa > 2.0mm) trong 48h tới.
    Sử dụng bảng silver_layer.alert_history để ghi nhớ trạng thái (Stateful Deduplication).
    Đảm bảo mỗi sự kiện dự báo mưa tại một địa điểm cụ thể chỉ được cảnh báo DUY NHẤT 1 LẦN.
    """

    def __init__(self):
        super().__init__()
        # Khởi tạo Telegram Alerter. Nếu cấu hình lỗi, Fail-Fast sẽ dừng job ngay tại đây.
        self.alerter = TelegramAlerter()

    def run_alert_check(self):
        self.connect()
        try:
            # 1. Truy vấn Dữ liệu Cảnh báo MỚI (Loại trừ những bản ghi đã có trong alert_history)
            # Query tập trung vào 48 giờ tới.
            # Không dùng Top 3 để đảm bảo KHÔNG BỎ SÓT bất kỳ khung giờ nguy hiểm nào.
            # Thay vào đó, Sắp xếp (ORDER BY) theo Quận trước, Giờ sau để hiển thị gọn gàng.
            query_new_alerts = """
                SELECT 
                    g.location_name, 
                    g.forecast_time, 
                    g.precipitation_probability, 
                    g.precipitation
                FROM gold_layer.mart_hourly_conditions g
                LEFT JOIN silver_layer.alert_history ah 
                    ON g.location_name = ah.location_name 
                    AND g.forecast_time = ah.forecast_time 
                    AND ah.alert_type = 'HEAVY_RAIN'
                WHERE g.forecast_time >= NOW() AT TIME ZONE 'Asia/Bangkok'
                  AND g.forecast_time <= (NOW() AT TIME ZONE 'Asia/Bangkok') + INTERVAL '48 hours'
                  AND g.precipitation_probability > 80
                  AND g.precipitation > 2.0
                  AND ah.id IS NULL -- Điều kiện CỐT LÕI: Chỉ lấy những record chưa từng cảnh báo
                ORDER BY g.location_name ASC, g.forecast_time ASC;
            """
            
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query_new_alerts)
                new_alerts = cursor.fetchall()
            
            if not new_alerts:
                logger.info("No NEW heavy rain events detected. Alert history muted spam.")
                return

            # 2. Định dạng tin nhắn chuẩn Production (Monospace Table, No Emojis, Clear Headers)
            message_lines = ["<pre>"]
            message_lines.append("[CẢNH BÁO] DỰ BÁO XÁC SUẤT & LƯỢNG MƯA TẠI TPHCM")
            message_lines.append("=================================================")
            message_lines.append("LOCATION       | TIME (BKK)  | PROB(%) | RAIN(MM)")
            message_lines.append("-------------------------------------------------")
            
            insert_history_records = []
            
            for r in new_alerts:
                # Xử lý chuỗi thông minh (Smart Abbreviation) để không bị cắt chữ
                loc_name = r["location_name"]
                loc_name = loc_name.replace("Thành phố ", "TP.")
                loc_name = loc_name.replace("Quận ", "Q.")
                loc_name = loc_name.replace("Huyện ", "H.")
                
                # Format string padding để tạo bảng thẳng hàng, căn lề chuẩn
                loc = loc_name[:14].ljust(14)
                time_str = r["forecast_time"].strftime("%m-%d %H:%M")
                prob = str(int(r["precipitation_probability"])).rjust(7)
                precip = f"{r['precipitation']:.1f}".rjust(8)
                
                message_lines.append(f"{loc} | {time_str} | {prob} | {precip}")
                
                # Chuẩn bị dữ liệu để insert vào alert_history
                insert_history_records.append((
                    r["location_name"],
                    r["forecast_time"],
                    'HEAVY_RAIN'
                ))

            message_lines.append("=================================================")
            message_lines.append("* Action: Monitor locations for heavy rainfall")
            message_lines.append("* gkinhere's Weather & Air quality Data Platform")
            message_lines.append("</pre>")
            
            final_message = "\n".join(message_lines)
            
            # 3. Giao quyền gửi tin nhắn cho TelegramAlerter
            logger.info(f"Attempting to send alert for {len(new_alerts)} new events.")
            success = self.alerter.send_message(final_message)
            
            # 4. Commit State (Ghi nhận lịch sử) CHỈ KHI gửi thành công
            if success:
                insert_query = """
                    INSERT INTO silver_layer.alert_history (location_name, forecast_time, alert_type)
                    VALUES %s
                    ON CONFLICT (location_name, forecast_time, alert_type) DO NOTHING;
                """
                with self.connection.cursor() as cursor:
                    execute_values(cursor, insert_query, insert_history_records)
                self.connection.commit()
                logger.info("Successfully updated silver_layer.alert_history.")
            else:
                # Nếu API Telegram lỗi (ví dụ rớt mạng), không commit DB. 
                # Lần chạy Airflow tiếp theo sẽ lấy lại chính data này để retry.
                self.connection.rollback()
                logger.error("Failed to send alert. State rollback applied (will retry next run).")

        except Error as e:
            self.connection.rollback()
            logger.error(f"Database error during alert check: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in alert job: {e}")
            raise
        finally:
            self.close()

if __name__ == "__main__":
    logger.info("Starting Stateful Weather Alert Job...")
    job = WeatherAlerterJob()
    job.run_alert_check()
    logger.info("Weather Alert Job finished.")

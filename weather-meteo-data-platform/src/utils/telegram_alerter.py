"""
telegram_alerter.py - Module chịu trách nhiệm tương tác với Telegram Bot API.
Tuân thủ OOP và thiết kế Separation of Concerns (Tách biệt trách nhiệm).
"""

import os
import requests
from src.utils.logger import get_logger

logger = get_logger("TelegramAlerter")

class TelegramAlerter:
    """
    Class quản lý việc gửi tin nhắn cảnh báo qua Telegram.
    Lấy thông tin xác thực từ Environment Variables (không hardcode).
    """

    def __init__(self):
        # 1. Lấy thông tin từ Biến Môi Trường (Environment Variables)
        self.bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")

        # 2. Fail-Fast: Kiểm tra ngay từ đầu, thiếu thì báo lỗi ngay.
        if not self.bot_token or not self.chat_id:
            logger.error("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in environment.")
            raise ValueError("Telegram credentials are not fully configured.")

        # 3. Khởi tạo Base URL của Telegram API
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

    def send_message(self, message: str) -> bool:
        """
        Gửi một tin nhắn text thuần túy đến Chat ID đã được cấu hình.
        
        Args:
            message (str): Nội dung cảnh báo cần gửi.
            
        Returns:
            bool: True nếu gửi thành công, False nếu thất bại.
        """
        url = f"{self.base_url}/sendMessage"
        
        # Payload data theo chuẩn REST API của Telegram
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML"
        }

        try:
            # Gửi HTTP POST request với timeout để tránh bị treo hệ thống
            response = requests.post(url, data=payload, timeout=10)
            
            # Kiểm tra HTTP Status Code (200 là thành công)
            if response.status_code == 200:
                logger.info("Telegram alert sent successfully.")
                return True
            else:
                logger.error(f"Failed to send Telegram alert. Status: {response.status_code}, Response: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            # Bắt lỗi mạng (Network Error, Timeout, DNS Fail...)
            logger.error(f"Network error while sending Telegram alert: {e}")
            return False

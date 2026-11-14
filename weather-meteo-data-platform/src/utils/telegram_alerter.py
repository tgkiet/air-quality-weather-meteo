"""Send forecast messages through Telegram's Bot API."""

import os

import requests

from src.utils.logger import get_logger

logger = get_logger("TelegramAlerter")


class TelegramDeliveryError(RuntimeError):
    """The API did not acknowledge successful delivery."""

    def __init__(self, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.retryable = retryable


class TelegramAlerter:
    def __init__(self):
        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        if not token or not self.chat_id:
            raise ValueError("Telegram credentials are not fully configured.")
        self.base_url = f"https://api.telegram.org/bot{token}"

    def send_message(self, message: str, *, chat_id=None, parse_mode="HTML") -> None:
        payload = {"chat_id": self.chat_id if chat_id is None else chat_id, "text": message}
        if parse_mode:
            payload["parse_mode"] = parse_mode

        try:
            response = requests.post(f"{self.base_url}/sendMessage", data=payload, timeout=10)
        except requests.exceptions.ConnectTimeout:
            # Requests documents ConnectTimeout as safe to retry.
            raise TelegramDeliveryError("Telegram connection timed out", retryable=True) from None
        except requests.exceptions.RequestException:
            # Request exception messages can contain the URL with the bot token.
            logger.warning("Telegram network error; delivery status unknown.")
            raise TelegramDeliveryError("Telegram delivery status unknown") from None

        try:
            body = response.json()
        except ValueError:
            body = None
        if response.status_code == 200 and isinstance(body, dict) and body.get("ok") is True:
            return
        if 400 <= response.status_code < 500 and isinstance(body, dict) and body.get("ok") is False:
            raise TelegramDeliveryError(
                f"Telegram rejected message (HTTP {response.status_code})", retryable=True,
            )
        raise TelegramDeliveryError(f"Telegram delivery status unknown (HTTP {response.status_code})")

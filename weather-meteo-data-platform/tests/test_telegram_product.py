from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import logging
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.scripts.alert_job import WeatherAlerterJob
from src.scripts.bot_services import BotFormatter
from src.scripts.telegram_bot import TelegramInteractiveBot
from src.utils.telegram_alerter import TelegramAlerter, TelegramDeliveryError


class TelegramProductTests(unittest.TestCase):
    def test_broadcast_failure_reaches_airflow(self):
        job = object.__new__(WeatherAlerterJob)
        job.connect = Mock()
        job.close = Mock()
        job.connection = Mock()
        now = datetime.now(timezone.utc)
        job.exec_date = now
        job.bkk_tz = timezone.utc
        job.run_sudden_alert = Mock(side_effect=RuntimeError("telegram delivery failed"))
        with patch("src.scripts.alert_job.config_manager") as config:
            config.alert_job_config = {"schedule_hours": {"morning": (now.hour + 1) % 24, "evening": (now.hour + 2) % 24}}
            with self.assertRaisesRegex(RuntimeError, "telegram delivery failed"):
                job.run_alert_check()
        job.connection.rollback.assert_called_once()
        job.close.assert_called_once()

    def test_missing_briefing_does_not_imply_pleasant_weather(self):
        job = object.__new__(WeatherAlerterJob)
        job.scheduled_region_prefix = "HN"
        job.exec_date = datetime.now(timezone.utc)
        job.connection = Mock()
        cursor = Mock()
        cursor.fetchall.return_value = []
        job.connection.cursor.return_value = nullcontext(cursor)
        job.alerter = Mock()
        job._send_once = Mock()
        job.run_holistic_briefing()
        msg = job._send_once.call_args.args[1]
        self.assertIn("Chưa đủ dữ liệu", msg)
        self.assertNotIn("lý tưởng", msg)

    def test_aq_distinguishes_old_fetch_time_and_forecast_time(self):
        formatter = BotFormatter(69, 2.0, 55.0, timezone.utc)
        valid_at = datetime(2026, 1, 15, 6, tzinfo=timezone.utc)
        fetched_at = valid_at - timedelta(hours=6)
        row = {
            "forecast_time": valid_at, "ingested_at": fetched_at,
            "fresh": False, "pm2_5": 49, "pm10": None,
            "pm2_5_level": "Không lành mạnh", "nitrogen_dioxide": None,
            "ozone": None, "is_air_quality_alert": False,
        }
        result = formatter.fmt_aqi("Điểm tra cứu", row, "vi")
        self.assertIn("Dữ liệu đã cũ", result)
        self.assertNotIn("49.0", result)
        self.assertNotIn("An toàn", result)

    def test_briefing_counts_partial_coverage_and_keeps_zero_rain(self):
        formatter = BotFormatter(69, 2.0, 55.0, timezone.utc)
        valid_at = datetime(2026, 1, 15, 6, tzinfo=timezone.utc)
        row = {
            "forecast_time": valid_at, "within_6h": True, "precipitation": 0,
            "precipitation_probability": 0, "temperature_2m": 22,
            "pm2_5": None, "weather_ingested_at": valid_at, "aq_ingested_at": None,
        }
        result = formatter.fmt_briefing("Hà Nội", [row], "vi")
        self.assertIn("1/6", result)
        self.assertIn("0%", result)
        self.assertIn("22.0", result)
        self.assertIn("Chưa đủ dữ liệu", result)

    def test_opt_in_requires_a_saved_point_in_private_chat(self):
        bot = object.__new__(TelegramInteractiveBot)
        bot._get_lang = Mock(return_value="en")
        bot.db = Mock()
        bot.db.set_subscription.return_value = False
        bot.bot = Mock()
        bot.cmd_subscribe(SimpleNamespace(chat=SimpleNamespace(id=43, type="private")))
        bot.db.set_subscription.assert_called_once_with(43, True)
        self.assertIn("Save a location", bot.bot.send_message.call_args.args[1])
        bot.db.reset_mock()
        bot.cmd_subscribe(SimpleNamespace(chat=SimpleNamespace(id=-43, type="group")))
        bot.db.set_subscription.assert_not_called()

    @patch("src.utils.telegram_alerter.requests.post")
    def test_network_failure_does_not_put_bot_token_in_logs(self, post):
        import requests
        post.side_effect = requests.Timeout("secret-token should never appear")
        alerter = object.__new__(TelegramAlerter)
        alerter.base_url = "https://example.invalid/botsecret-token"
        alerter.chat_id = "43"
        with self.assertLogs("TelegramAlerter", logging.WARNING) as logs:
            with self.assertRaisesRegex(RuntimeError, "unknown"):
                alerter.send_message("test")
        self.assertNotIn("secret-token", "\n".join(logs.output))

    def test_sudden_alert_reads_named_cursor_results(self):
        for previous, should_query in (
            (None, True), ({"delivered_at": datetime.now(timezone.utc)}, False),
        ):
            with self.subTest(previous=previous):
                job, cursor = self._delivery_job()
                job.sudden_region_prefix = "HCM"
                cursor.fetchone.side_effect = [{"delivery_table": "telegram_delivery"}, previous]
                cursor.fetchall.return_value = []
                job.run_sudden_alert()
                self.assertEqual(cursor.fetchall.called, should_query)
                job.alerter.send_message.assert_not_called()

    def test_confirmed_delivery_is_not_sent_again(self):
        job, cursor = self._delivery_job()
        cursor.fetchone.side_effect = [
            ("telegram_delivery",), None, (job.exec_date,),
        ]
        job._send_once("daily", "forecast", chat_id=43)
        job.alerter.send_message.assert_not_called()

    def test_unknown_delivery_survives_retry_without_resending(self):
        job, cursor = self._delivery_job()
        cursor.fetchone.side_effect = [
            ("telegram_delivery",), ("delivery_id",),
            ("telegram_delivery",), None, (None,),
        ]
        job.alerter.send_message.side_effect = TelegramDeliveryError("status unknown")
        for _ in range(2):
            with self.assertRaisesRegex(TelegramDeliveryError, "status unknown"):
                job._send_once("daily", "forecast", chat_id=43)
        job.alerter.send_message.assert_called_once()

    def test_rejected_delivery_releases_its_claim_for_retry(self):
        job, cursor = self._delivery_job()
        cursor.fetchone.side_effect = [("telegram_delivery",), ("delivery_id",)]
        job.alerter.send_message.side_effect = TelegramDeliveryError("rejected", retryable=True)
        with self.assertRaisesRegex(TelegramDeliveryError, "rejected"):
            job._send_once("daily", "forecast", chat_id=43)
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertTrue(any(sql.startswith("DELETE FROM silver_layer.telegram_delivery") for sql in statements))

    @staticmethod
    def _delivery_job():
        job = object.__new__(WeatherAlerterJob)
        job.exec_date = datetime(2026, 11, 14, 0, tzinfo=timezone.utc)
        job.alerter = Mock()
        job.connection = Mock()
        cursor = Mock()
        job.connection.cursor.return_value = nullcontext(cursor)
        return job, cursor

    def test_menu_uses_all_configured_locations_and_old_numeric_buttons(self):
        from src.utils.config_manager import config_manager

        with (
            patch("src.scripts.telegram_bot.BasePostgresLoader.__init__", return_value=None),
            patch.dict("os.environ", {"TELEGRAM_BOT_TOKEN": "test-token"}),
            patch("src.scripts.telegram_bot.telebot.TeleBot"),
            patch("src.scripts.telegram_bot.BotDatabaseManager"),
            patch.object(TelegramInteractiveBot, "_register_slash_commands"),
        ):
            bot = TelegramInteractiveBot()

        self.assertEqual(set(bot.districts), {row["name"] for row in config_manager.locations})
        self.assertEqual(bot._resolve_location("0"), bot.legacy_districts[0]["db_name"])
        for city in bot.cities:
            keyboard = bot._kb_districts("home", city["id"], "vi")
            entries = [button.callback_data for row in keyboard.keyboard for button in row]
            expected = {
                f"home|{location}" for location in bot.districts if location.startswith(city["id"] + " ")
            }
            self.assertEqual({entry for entry in entries if entry.startswith("home|")}, expected)
            self.assertTrue(all(len(entry.encode("utf-8")) <= 64 for entry in entries))

    def test_24_hour_weather_message_fits_telegram_and_keeps_missing_metrics(self):
        formatter = BotFormatter(69, 2.0, 55.0, timezone.utc)
        valid_at = datetime(2026, 1, 15, 6, tzinfo=timezone.utc)
        rows = [
            {
                "forecast_time": valid_at + timedelta(hours=hour), "ingested_at": valid_at,
                "temperature_2m": 22, "precipitation": None, "precipitation_probability": None,
                "wind_speed_10m": None, "weather_uv_index": None,
            }
            for hour in range(24)
        ]
        text = formatter.fmt_weather("HCM Thành phố Thủ Đức", rows, "vi", limit=24)
        self.assertIn("· 24/24", text)
        self.assertEqual(text.count("Mưa N/A"), 24)
        self.assertLess(len(text), 4096)

"""
telegram_bot.py  –  Interactive Pull Bot (Dual-Core Architecture, Pull side).

Design principles:
  - Strict OOP: Controller pattern separating presentation and database logic.
  - Zero hardcode: thresholds and district list from config_manager.
  - Exact DB matching via index-based callback_data ("weather|3")
    → eliminates ILIKE ambiguity.
  - Bilingual Support (EN/VI) with user preference stored in DB.
  - Provides practical human-readable insights alongside raw data.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import requests
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

from src.loaders.base_loader import BasePostgresLoader
from src.utils.logger import get_logger
from src.utils.config_manager import config_manager
from src.scripts.bot_services import BotDatabaseManager, BotFormatter

logger = get_logger("TelegramBot")
_CB_SEP = "|"

class TelegramInteractiveBot(BasePostgresLoader):
    def __init__(self):
        super().__init__()
        load_dotenv()

        self.bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        if not self.bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN is not configured in environment.")

        self.bot = telebot.TeleBot(self.bot_token, parse_mode=None)
        bkk_tz = ZoneInfo("Asia/Bangkok")

        bot_cfg    = config_manager.telegram_bot_config
        thresholds = config_manager.alert_thresholds
        try:
            self.districts          = bot_cfg["districts"]
            rain_prob_threshold     = int(thresholds["rain_probability_pct"])
            rain_mm_threshold       = float(thresholds["rain_mm"])
            pm25_alert_threshold    = float(thresholds["pm25_alert_ugm3"])
        except KeyError as e:
            raise ValueError(f"Missing required telegram_bot config: {e}")

        if not self.districts:
            raise ValueError("No districts configured in telegram_bot.districts.")

        # Initialize modular services
        self.db = BotDatabaseManager(self)
        self.formatter = BotFormatter(rain_prob_threshold, rain_mm_threshold, pm25_alert_threshold, bkk_tz)
        
        self._lang_cache: dict[int, str] = {}
        self.db.init_lang_table()

        self._register_slash_commands()
        self._register_handlers()

    def _get_lang(self, chat_id: int) -> str:
        if chat_id in self._lang_cache:
            return self._lang_cache[chat_id]
        lang = self.db.get_user_lang(chat_id)
        self._lang_cache[chat_id] = lang
        return lang

    def _set_lang(self, chat_id: int, lang: str):
        self.db.set_user_lang(chat_id, lang)
        self._lang_cache[chat_id] = lang

    def _register_slash_commands(self) -> None:
        url = f"https://api.telegram.org/bot{self.bot_token}/setMyCommands"
        commands = [
            {"command": "start",   "description": "Language & Guide / Đổi ngôn ngữ & HD"},
            {"command": "menu",    "description": "Menu / Mở danh mục"},
            {"command": "weather", "description": "Weather / Dự báo thời tiết"},
            {"command": "aqi",     "description": "Air quality / Chất lượng không khí"},
        ]
        try:
            requests.post(url, json={"commands": commands}, timeout=5)
        except requests.exceptions.RequestException:
            pass

    def _register_handlers(self) -> None:
        self.bot.message_handler(commands=["start"])(self.cmd_start)
        self.bot.message_handler(commands=["menu"])(self.cmd_menu)
        self.bot.message_handler(commands=["weather"])(self.cmd_weather)
        self.bot.message_handler(commands=["aqi"])(self.cmd_aqi)

        self.bot.callback_query_handler(func=lambda c: c.data == "main_menu")(self.cb_main_menu)
        self.bot.callback_query_handler(func=lambda c: c.data == "sel_weather")(self.cb_sel_weather)
        self.bot.callback_query_handler(func=lambda c: c.data == "sel_aqi")(self.cb_sel_aqi)
        self.bot.callback_query_handler(func=lambda c: c.data.startswith(f"weather{_CB_SEP}"))(self.cb_show_weather)
        self.bot.callback_query_handler(func=lambda c: c.data.startswith(f"aqi{_CB_SEP}"))(self.cb_show_aqi)
        self.bot.callback_query_handler(func=lambda c: c.data.startswith(f"lang{_CB_SEP}"))(self.cb_set_lang)

    # ================================================================
    # KEYBOARDS
    # ================================================================

    def _kb_main(self, lang: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=2)
        if lang == "en":
            kb.add(
                InlineKeyboardButton("Weather Forecast", callback_data="sel_weather"),
                InlineKeyboardButton("Air Quality (AQI)", callback_data="sel_aqi"),
            )
        else:
            kb.add(
                InlineKeyboardButton("Dự Báo Thời Tiết", callback_data="sel_weather"),
                InlineKeyboardButton("Chất Lượng Không Khí", callback_data="sel_aqi"),
            )
        return kb

    def _kb_districts(self, action: str, lang: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=2)
        buttons = [
            InlineKeyboardButton(d["label"], callback_data=f"{action}{_CB_SEP}{i}")
            for i, d in enumerate(self.districts)
        ]
        kb.add(*buttons)
        btn_text = "<< Main Menu" if lang == "en" else "<< Menu Chính"
        kb.add(InlineKeyboardButton(btn_text, callback_data="main_menu"))
        return kb

    def _kb_after_result(self, idx: int, current_action: str, lang: str) -> InlineKeyboardMarkup:
        other  = "aqi" if current_action == "weather" else "weather"
        kb = InlineKeyboardMarkup(row_width=1)
        if lang == "en":
            o_label = "Air Quality (AQI)" if other == "aqi" else "Weather Forecast"
            kb.add(
                InlineKeyboardButton(f"View {o_label} here", callback_data=f"{other}{_CB_SEP}{idx}"),
                InlineKeyboardButton("<< Select Another District", callback_data=f"sel_{current_action}"),
                InlineKeyboardButton("<< Main Menu", callback_data="main_menu"),
            )
        else:
            o_label = "Chất Lượng Không Khí" if other == "aqi" else "Dự Báo Thời Tiết"
            kb.add(
                InlineKeyboardButton(f"Xem {o_label} quận này", callback_data=f"{other}{_CB_SEP}{idx}"),
                InlineKeyboardButton("<< Chọn Quận Khác", callback_data=f"sel_{current_action}"),
                InlineKeyboardButton("<< Menu Chính", callback_data="main_menu"),
            )
        return kb

    # ================================================================
    # HANDLERS
    # ================================================================

    def cmd_start(self, message) -> None:
        name = message.from_user.first_name or "there"
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("English",    callback_data=f"lang{_CB_SEP}en"),
            InlineKeyboardButton("Tiếng Việt", callback_data=f"lang{_CB_SEP}vi"),
        )
        self.bot.send_message(
            message.chat.id,
            f"Hi {name}! Please select your language / Vui lòng chọn ngôn ngữ:",
            reply_markup=kb,
        )

    def cmd_menu(self, message) -> None:
        lang = self._get_lang(message.chat.id)
        text = "Select a feature:\n(Type /start to change language or view full guide)" if lang == "en" \
               else "Chọn một chức năng:\n(Gõ /start để đổi ngôn ngữ hoặc xem hướng dẫn)"
        self.bot.send_message(message.chat.id, text, reply_markup=self._kb_main(lang))

    def cmd_weather(self, message) -> None:
        lang = self._get_lang(message.chat.id)
        text = "WEATHER FORECAST\nSelect a district:" if lang == "en" else "DỰ BÁO THỜI TIẾT\nChọn quận/huyện:"
        self.bot.send_message(message.chat.id, text, reply_markup=self._kb_districts("weather", lang))

    def cmd_aqi(self, message) -> None:
        lang = self._get_lang(message.chat.id)
        text = "AIR QUALITY INDEX\nSelect a district:" if lang == "en" else "CHẤT LƯỢNG KHÔNG KHÍ\nChọn quận/huyện:"
        self.bot.send_message(message.chat.id, text, reply_markup=self._kb_districts("aqi", lang))

    def cb_set_lang(self, call) -> None:
        _, lang = call.data.split(_CB_SEP, 1)
        self._set_lang(call.message.chat.id, lang)
        self.bot.answer_callback_query(call.id, text="Language saved!" if lang == "en" else "Đã lưu ngôn ngữ!")
        
        name = call.from_user.first_name or "there"
        text = self.formatter.get_guide_text(lang, name)
        
        self.bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text=text,
            reply_markup=self._kb_main(lang)
        )

    def cb_main_menu(self, call) -> None:
        lang = self._get_lang(call.message.chat.id)
        self.bot.answer_callback_query(call.id)
        text = "Select a feature:\n(Type /start to change language or view full guide)" if lang == "en" \
               else "Chọn một chức năng:\n(Gõ /start để đổi ngôn ngữ hoặc xem hướng dẫn)"
        self.bot.send_message(call.message.chat.id, text, reply_markup=self._kb_main(lang))

    def cb_sel_weather(self, call) -> None:
        lang = self._get_lang(call.message.chat.id)
        self.bot.answer_callback_query(call.id)
        text = "WEATHER FORECAST\nSelect a district:" if lang == "en" else "DỰ BÁO THỜI TIẾT\nChọn quận/huyện:"
        self.bot.send_message(call.message.chat.id, text, reply_markup=self._kb_districts("weather", lang))

    def cb_sel_aqi(self, call) -> None:
        lang = self._get_lang(call.message.chat.id)
        self.bot.answer_callback_query(call.id)
        text = "AIR QUALITY INDEX\nSelect a district:" if lang == "en" else "CHẤT LƯỢNG KHÔNG KHÍ\nChọn quận/huyện:"
        self.bot.send_message(call.message.chat.id, text, reply_markup=self._kb_districts("aqi", lang))

    def cb_show_weather(self, call) -> None:
        lang = self._get_lang(call.message.chat.id)
        _, idx_str = call.data.split(_CB_SEP, 1)
        idx        = int(idx_str)
        district   = self.districts[idx]
        
        self.bot.answer_callback_query(call.id, text=f"Loading {district['label']}..." if lang == "en" else f"Đang tải {district['label']}...")

        rows = self.db.query_weather(district["db_name"])
        if not rows:
            not_found = f"No weather data found for {district['label']}.\nPlease try again later." if lang == "en" \
                        else f"Không tìm thấy dữ liệu thời tiết cho {district['label']}.\nVui lòng thử lại sau."
            self.bot.send_message(call.message.chat.id, not_found, reply_markup=self._kb_districts("weather", lang))
            return

        text = self.formatter.fmt_weather(district["label"], rows, lang)
        self.bot.send_message(call.message.chat.id, text, reply_markup=self._kb_after_result(idx, "weather", lang))

    def cb_show_aqi(self, call) -> None:
        lang = self._get_lang(call.message.chat.id)
        _, idx_str = call.data.split(_CB_SEP, 1)
        idx        = int(idx_str)
        district   = self.districts[idx]
        
        self.bot.answer_callback_query(call.id, text=f"Loading {district['label']}..." if lang == "en" else f"Đang tải {district['label']}...")

        row = self.db.query_aqi(district["db_name"])
        if not row:
            not_found = f"No AQI data found for {district['label']}.\nPlease try again later." if lang == "en" \
                        else f"Không tìm thấy dữ liệu AQI cho {district['label']}.\nVui lòng thử lại sau."
            self.bot.send_message(call.message.chat.id, not_found, reply_markup=self._kb_districts("aqi", lang))
            return

        text = self.formatter.fmt_aqi(district["label"], row, lang)
        self.bot.send_message(call.message.chat.id, text, reply_markup=self._kb_after_result(idx, "aqi", lang))

    def run(self) -> None:
        logger.info("Telegram Interactive Bot is running...")
        try:
            self.bot.infinity_polling(timeout=30, long_polling_timeout=20)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            raise
        finally:
            self.close()

if __name__ == "__main__":
    bot_app = TelegramInteractiveBot()
    bot_app.run()

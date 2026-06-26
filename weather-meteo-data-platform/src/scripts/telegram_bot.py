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

        self.bot = telebot.TeleBot(self.bot_token, parse_mode=None, num_threads=4)
        bkk_tz = ZoneInfo("Asia/Bangkok")

        bot_cfg    = config_manager.telegram_bot_config
        thresholds = config_manager.alert_thresholds
        try:
            self.cities             = bot_cfg["cities"]
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
        self.bot.callback_query_handler(func=lambda c: c.data.startswith(f"weather{_CB_SEP}"))(self.cb_sel_time_weather)
        self.bot.callback_query_handler(func=lambda c: c.data.startswith(f"show_wx{_CB_SEP}"))(self.cb_show_weather)
        self.bot.callback_query_handler(func=lambda c: c.data.startswith(f"aqi{_CB_SEP}"))(self.cb_show_aqi)
        self.bot.callback_query_handler(func=lambda c: c.data.startswith(f"lang{_CB_SEP}"))(self.cb_set_lang)
        self.bot.callback_query_handler(func=lambda c: c.data.startswith(f"city{_CB_SEP}"))(self.cb_show_districts)

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

    def _kb_cities(self, action: str, lang: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=2)
        buttons = []
        for c in self.cities:
            label = c["label_en"] if lang == "en" else c["label_vi"]
            buttons.append(InlineKeyboardButton(label, callback_data=f"city{_CB_SEP}{action}{_CB_SEP}{c['id']}"))
        kb.add(*buttons)
        btn_text = "<< Main Menu" if lang == "en" else "<< Menu Chính"
        kb.add(InlineKeyboardButton(btn_text, callback_data="main_menu"))
        return kb

    def _kb_districts(self, action: str, city: str, lang: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=2)
        buttons = []
        for d in self.districts:
            if d["db_name"].startswith(f"{city} "):
                buttons.append(InlineKeyboardButton(d["label"], callback_data=f"{action}{_CB_SEP}{d['db_name']}"))
        kb.add(*buttons)
        btn_text = "<< Back to Cities" if lang == "en" else "<< Chọn Thành Phố"
        kb.add(InlineKeyboardButton(btn_text, callback_data=f"sel_{action}"))
        return kb

    def _kb_after_result(self, db_name: str, current_action: str, lang: str) -> InlineKeyboardMarkup:
        other  = "aqi" if current_action == "weather" else "weather"
        city_prefix = db_name.split(" ")[0]
        kb = InlineKeyboardMarkup(row_width=1)
        if lang == "en":
            o_label = "Air Quality (AQI)" if other == "aqi" else "Weather Forecast"
            kb.add(
                InlineKeyboardButton(f"View {o_label} here", callback_data=f"{other}{_CB_SEP}{db_name}"),
                InlineKeyboardButton("<< Select Another District", callback_data=f"city{_CB_SEP}{current_action}{_CB_SEP}{city_prefix}"),
                InlineKeyboardButton("<< Main Menu", callback_data="main_menu"),
            )
        else:
            o_label = "Chất Lượng Không Khí" if other == "aqi" else "Dự Báo Thời Tiết"
            kb.add(
                InlineKeyboardButton(f"Xem {o_label} quận này", callback_data=f"{other}{_CB_SEP}{db_name}"),
                InlineKeyboardButton("<< Chọn Quận Khác", callback_data=f"city{_CB_SEP}{current_action}{_CB_SEP}{city_prefix}"),
                InlineKeyboardButton("<< Menu Chính", callback_data="main_menu"),
            )
        return kb

    def _kb_time_options(self, db_name: str, lang: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=1)
        city_prefix = db_name.split(" ")[0]
        if lang == "en":
            kb.add(
                InlineKeyboardButton("Next 6 Hours", callback_data=f"show_wx{_CB_SEP}6{_CB_SEP}0{_CB_SEP}{db_name}"),
                InlineKeyboardButton("Next 12 Hours", callback_data=f"show_wx{_CB_SEP}12{_CB_SEP}0{_CB_SEP}{db_name}"),
                InlineKeyboardButton("Next 24 Hours", callback_data=f"show_wx{_CB_SEP}24{_CB_SEP}0{_CB_SEP}{db_name}"),
                InlineKeyboardButton("<< Back", callback_data=f"city{_CB_SEP}weather{_CB_SEP}{city_prefix}")
            )
        else:
            kb.add(
                InlineKeyboardButton("6 Giờ tới", callback_data=f"show_wx{_CB_SEP}6{_CB_SEP}0{_CB_SEP}{db_name}"),
                InlineKeyboardButton("12 Giờ tới", callback_data=f"show_wx{_CB_SEP}12{_CB_SEP}0{_CB_SEP}{db_name}"),
                InlineKeyboardButton("24 Giờ tới", callback_data=f"show_wx{_CB_SEP}24{_CB_SEP}0{_CB_SEP}{db_name}"),
                InlineKeyboardButton("<< Quay lại", callback_data=f"city{_CB_SEP}weather{_CB_SEP}{city_prefix}")
            )
        return kb

    def _kb_after_result_wx(self, db_name: str, limit: int, offset: int, lang: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=1)
        city_prefix = db_name.split(" ")[0]
        
        # Dynamic Pagination Logic (Preventing Telegram 4096 char limit)
        nav_buttons = []
        if offset >= limit:
            nav_buttons.append(InlineKeyboardButton("<< Previous" if lang == "en" else "<< Xem trước", 
                                            callback_data=f"show_wx{_CB_SEP}{limit}{_CB_SEP}{offset-limit}{_CB_SEP}{db_name}"))
        
        # Open-Meteo allows up to 7 days forecast, so offset < 72 is safe
        if offset < 72:
            nav_buttons.append(InlineKeyboardButton("Next >>" if lang == "en" else "Xem tiếp >>", 
                                            callback_data=f"show_wx{_CB_SEP}{limit}{_CB_SEP}{offset+limit}{_CB_SEP}{db_name}"))
            
        if nav_buttons:
            kb.row(*nav_buttons)
            
        if lang == "en":
            kb.add(
                InlineKeyboardButton("Air Quality (AQI)", callback_data=f"aqi{_CB_SEP}{db_name}"),
                InlineKeyboardButton("<< Select Timeframe", callback_data=f"weather{_CB_SEP}{db_name}"),
                InlineKeyboardButton("<< Main Menu", callback_data="main_menu"),
            )
        else:
            kb.add(
                InlineKeyboardButton("Chất Lượng Không Khí", callback_data=f"aqi{_CB_SEP}{db_name}"),
                InlineKeyboardButton("<< Chọn Lại Mốc Thời Gian", callback_data=f"weather{_CB_SEP}{db_name}"),
                InlineKeyboardButton("<< Menu Chính", callback_data="main_menu"),
            )
        return kb

    def _safe_edit_message(self, text, chat_id, message_id, reply_markup):
        import time
        try:
            self.bot.edit_message_text(text, chat_id, message_id, reply_markup=reply_markup)
        except Exception as e:
            err_str = str(e).lower()
            if "too many requests" in err_str or "retry after" in err_str:
                logger.warning("Telegram rate limit hit (429). Sleeping for 1.5s and retrying...")
                time.sleep(1.5)
                try:
                    self.bot.edit_message_text(text, chat_id, message_id, reply_markup=reply_markup)
                except Exception as inner_e:
                    logger.error(f"Failed to edit message after retry: {inner_e}")
            elif "message is not modified" in err_str:
                pass # Ignore identical edits
            else:
                logger.error(f"Error editing message: {e}")

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
        text = "WEATHER FORECAST\nSelect a city:" if lang == "en" else "DỰ BÁO THỜI TIẾT\nChọn thành phố:"
        self.bot.send_message(message.chat.id, text, reply_markup=self._kb_cities("weather", lang))

    def cmd_aqi(self, message) -> None:
        lang = self._get_lang(message.chat.id)
        text = "AIR QUALITY INDEX\nSelect a city:" if lang == "en" else "CHẤT LƯỢNG KHÔNG KHÍ\nChọn thành phố:"
        self.bot.send_message(message.chat.id, text, reply_markup=self._kb_cities("aqi", lang))

    def cb_set_lang(self, call) -> None:
        _, lang = call.data.split(_CB_SEP, 1)
        self._set_lang(call.message.chat.id, lang)
        self.bot.answer_callback_query(call.id, text="Language saved!" if lang == "en" else "Đã lưu ngôn ngữ!")
        
        name = call.from_user.first_name or "there"
        text = self.formatter.get_guide_text(lang, name)
        
        self._safe_edit_message(
            text=text,
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=self._kb_main(lang)
        )

    def cb_main_menu(self, call) -> None:
        self.bot.answer_callback_query(call.id)
        lang = self._get_lang(call.message.chat.id)
        text = "Select a feature:\n(Type /start to change language or view full guide)" if lang == "en" \
               else "Chọn một chức năng:\n(Gõ /start để đổi ngôn ngữ hoặc xem hướng dẫn)"
        self._safe_edit_message(text, call.message.chat.id, call.message.message_id, reply_markup=self._kb_main(lang))

    def cb_sel_weather(self, call) -> None:
        self.bot.answer_callback_query(call.id)
        lang = self._get_lang(call.message.chat.id)
        text = "WEATHER FORECAST\nSelect a city:" if lang == "en" else "DỰ BÁO THỜI TIẾT\nChọn thành phố:"
        self._safe_edit_message(text, call.message.chat.id, call.message.message_id, reply_markup=self._kb_cities("weather", lang))

    def cb_sel_aqi(self, call) -> None:
        self.bot.answer_callback_query(call.id)
        lang = self._get_lang(call.message.chat.id)
        text = "AIR QUALITY INDEX\nSelect a city:" if lang == "en" else "CHẤT LƯỢNG KHÔNG KHÍ\nChọn thành phố:"
        self._safe_edit_message(text, call.message.chat.id, call.message.message_id, reply_markup=self._kb_cities("aqi", lang))

    def cb_show_districts(self, call) -> None:
        self.bot.answer_callback_query(call.id)
        lang = self._get_lang(call.message.chat.id)
        _, action, city = call.data.split(_CB_SEP)
        feature = "WEATHER" if action == "weather" else "AQI"
        feature_vn = "DỰ BÁO THỜI TIẾT" if action == "weather" else "CHẤT LƯỢNG KHÔNG KHÍ"
        
        city_label = city
        for c in self.cities:
            if c["id"] == city:
                city_label = c["label_en"] if lang == "en" else c["label_vi"]
                break
                
        text = f"{feature} - {city_label}\nSelect a district:" if lang == "en" else f"{feature_vn} - {city_label}\nChọn quận/huyện:"
        self._safe_edit_message(text, call.message.chat.id, call.message.message_id, reply_markup=self._kb_districts(action, city, lang))

    def cb_sel_time_weather(self, call) -> None:
        self.bot.answer_callback_query(call.id)
        lang = self._get_lang(call.message.chat.id)
        _, db_name = call.data.split(_CB_SEP, 1)
        
        # Handle backward compat for db_name if needed
        if db_name.isdigit():
            idx = int(db_name)
            if idx < len(self.districts):
                db_name = self.districts[idx]["db_name"]
            else:
                self.bot.answer_callback_query(call.id, text="Lỗi dữ liệu nút bấm cũ.")
                return
                
        district = next((d for d in self.districts if d["db_name"] == db_name), None)
        if not district:
            self.bot.answer_callback_query(call.id, text="Khu vực không tồn tại.")
            return

        text = f"Select forecast duration for {district['label']}:" if lang == "en" else f"Chọn mốc thời gian dự báo cho {district['label']}:"
        self._safe_edit_message(text, call.message.chat.id, call.message.message_id, reply_markup=self._kb_time_options(db_name, lang))

    def cb_show_weather(self, call) -> None:
        lang = self._get_lang(call.message.chat.id)
        _, limit_str, offset_str, db_name = call.data.split(_CB_SEP, 3)
        limit = int(limit_str)
        offset = int(offset_str)
        
        district = next((d for d in self.districts if d["db_name"] == db_name), None)
        if not district:
            self.bot.answer_callback_query(call.id, text="Khu vực không tồn tại.")
            return
        
        self.bot.answer_callback_query(call.id, text=f"Loading {district['label']}..." if lang == "en" else f"Đang tải {district['label']}...")

        rows = self.db.query_weather(db_name, limit, offset)
        if not rows:
            city = db_name.split(" ")[0]
            not_found = f"No weather data found for {district['label']}.\nPlease try again later." if lang == "en" \
                        else f"Không tìm thấy dữ liệu thời tiết cho {district['label']}.\nVui lòng thử lại sau."
            self._safe_edit_message(not_found, call.message.chat.id, call.message.message_id, reply_markup=self._kb_time_options(db_name, lang))
            return

        text = self.formatter.fmt_weather(district["label"], rows, lang, limit, offset)
        self._safe_edit_message(text, call.message.chat.id, call.message.message_id, reply_markup=self._kb_after_result_wx(db_name, limit, offset, lang))

    def cb_show_aqi(self, call) -> None:
        lang = self._get_lang(call.message.chat.id)
        _, db_name = call.data.split(_CB_SEP, 1)
        
        if db_name.isdigit():
            idx = int(db_name)
            if idx < len(self.districts):
                district = self.districts[idx]
                db_name = district["db_name"]
            else:
                self.bot.answer_callback_query(call.id, text="Lỗi dữ liệu nút bấm cũ, vui lòng quay lại menu.")
                return
        else:
            district = next((d for d in self.districts if d["db_name"] == db_name), None)
            if not district:
                self.bot.answer_callback_query(call.id, text="Khu vực không tồn tại.")
                return
        
        self.bot.answer_callback_query(call.id, text=f"Loading {district['label']}..." if lang == "en" else f"Đang tải {district['label']}...")

        row = self.db.query_aqi(db_name)
        if not row:
            city = db_name.split(" ")[0]
            not_found = f"No AQI data found for {district['label']}.\nPlease try again later." if lang == "en" \
                        else f"Không tìm thấy dữ liệu AQI cho {district['label']}.\nVui lòng thử lại sau."
            self._safe_edit_message(not_found, call.message.chat.id, call.message.message_id, reply_markup=self._kb_districts("aqi", city, lang))
            return

        text = self.formatter.fmt_aqi(district["label"], row, lang)
        self._safe_edit_message(text, call.message.chat.id, call.message.message_id, reply_markup=self._kb_after_result(db_name, "aqi", lang))

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

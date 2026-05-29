import json
import os
import time
from src.utils.logger import get_logger

logger = get_logger("ConfigManager")

class ConfigManager:
    """
    Singleton class to manage and provide runtime configurations with TTL cache.
    """
    _instance = None
    _config = None
    _api_config_file = None
    _last_load_time = 0
    _cache_ttl = 60  # Auto reload from disk every 60 seconds

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._config = {}
            cls._instance._api_config_file = {}
            cls._instance._last_load_time = 0
            cls._instance._cache_ttl = 60
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        current_time = time.time()
        # Return immediately if cache is not expired and data exists
        if current_time - self._last_load_time < self._cache_ttl and self._config:
            return

        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        runtime_path = os.path.join(base_dir, 'config', 'config_runtime_constant.json')
        api_path = os.path.join(base_dir, 'config', 'config.json')
        
        try:
            with open(runtime_path, 'r', encoding='utf-8') as f:
                runtime_data = json.load(f)
                if not isinstance(runtime_data, dict):
                    raise ValueError("config_runtime_constant.json root must be a dictionary.")
                self._config = runtime_data
                
            with open(api_path, 'r', encoding='utf-8') as f:
                api_data = json.load(f)
                if not isinstance(api_data, dict):
                    raise ValueError("config.json root must be a dictionary.")
                self._api_config_file = api_data
                
            logger.info("Successfully loaded/reloaded configurations from disk.")
        except Exception as e:
            logger.error(f"Failed to load configs: {e}. Keeping old state if available.")
            if not self._config:
                self._config = {}
            if not self._api_config_file:
                self._api_config_file = {}
        finally:
            self._last_load_time = current_time

    @property
    def api_config(self) -> dict:
        self._load_config()
        return self._config.get("api", {})

    @property
    def database_config(self) -> dict:
        self._load_config()
        return self._config.get("database", {})

    @property
    def telegram_bot_config(self) -> dict:
        self._load_config()
        return self._config.get("telegram_bot", {})

    @property
    def alert_thresholds(self) -> dict:
        self._load_config()
        return self._config.get("alert_thresholds", {})

    @property
    def alert_job_config(self) -> dict:
        self._load_config()
        return self._config.get("alert_job", {})

    # New properties for config.json (Phase 2 SRP fix)
    @property
    def open_meteo_api(self) -> dict:
        self._load_config()
        return self._api_config_file.get("api", {}).get("open_meteo", {})
        
    @property
    def locations(self) -> list:
        self._load_config()
        return self._api_config_file.get("locations", [])

# Khởi tạo sẵn một object để các class khác import và sử dụng trực tiếp
config_manager = ConfigManager()

import json
import os
from src.utils.logger import get_logger

logger = get_logger("ConfigManager")

class ConfigManager:
    """
    Singleton class to manage and provide runtime configurations.
    """
    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        # Đường dẫn tuyệt đối đến file config_runtime_constant.json
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_dir, 'config', 'config_runtime_constant.json')
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    raise ValueError("JSON root must be a dictionary.")
                self._config = data
            logger.info("Successfully loaded runtime configurations.")
        except Exception as e:
            logger.error(f"Failed to load config at {config_path}: {e}. Returning empty configurations.")
            self._config = {}

    @property
    def api_config(self) -> dict:
        return self._config.get("api", {})

    @property
    def database_config(self) -> dict:
        return self._config.get("database", {})

    @property
    def telegram_bot_config(self) -> dict:
        return self._config.get("telegram_bot", {})

    @property
    def alert_thresholds(self) -> dict:
        return self._config.get("alert_thresholds", {})

    @property
    def alert_job_config(self) -> dict:
        return self._config.get("alert_job", {})

# Khởi tạo sẵn một object để các class khác import và sử dụng trực tiếp
config_manager = ConfigManager()

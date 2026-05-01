import json
import os
from src.utils.logger import get_logger

logger = get_logger("ConfigManager")

class ConfigManager:
    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json'))
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._config = json.load(f)
            logger.info("Successfully loaded configuration from config.json")
        except Exception as e:
            logger.error(f"Failed to load config.json: {e}")
            raise Exception("Critical Error: Missing configuration file.") from e

    def get_config(self):
        return self._config

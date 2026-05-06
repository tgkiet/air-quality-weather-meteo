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
                self._config = json.load(f)
            logger.info("Successfully loaded runtime configurations.")
        except FileNotFoundError:
            logger.warning(f"Config file not found at {config_path}. Using default values.")
            self._config = {
                "api": {"max_retries": 3, "backoff_factor": 1, "timeout_sec": 10},
                "database": {"max_retries": 3, "retry_delay_sec": 5}
            }
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing config file: {e}. Using default values.")
            self._config = {
                "api": {"max_retries": 3, "backoff_factor": 1, "timeout_sec": 10},
                "database": {"max_retries": 3, "retry_delay_sec": 5}
            }

    @property
    def api_config(self) -> dict:
        return self._config.get("api", {})

    @property
    def database_config(self) -> dict:
        return self._config.get("database", {})

# Khởi tạo sẵn một object để các class khác import và sử dụng trực tiếp
config_manager = ConfigManager()

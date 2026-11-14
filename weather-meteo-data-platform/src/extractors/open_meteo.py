from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.config_manager import config_manager
from src.utils.logger import get_logger

logger = get_logger("OpenMeteoExtractor")


class OpenMeteoExtractor:
    def __init__(self, url: str):
        self.url = url
        self.session = requests.Session()
        api_config = config_manager.api_config
        # The only POSTs made by this session are read-only forecast queries.
        retry_strategy = Retry(
            total=api_config.get("max_retries", 3),
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods={"POST"},
            backoff_factor=api_config.get("backoff_factor", 1),
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        self.session.mount("http://", HTTPAdapter(max_retries=retry_strategy))

    def get_open_meteo_data(self, params: dict, expected_keys: set | None = None) -> dict | list:
        timeout = config_manager.api_config.get("timeout_sec", 10)
        try:
            # The batched coordinate list is sent in the body to keep the URL short.
            response = self.session.post(self.url, data=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            items = data if isinstance(data, list) else [data]
            if not items or any(not isinstance(item, dict) for item in items):
                raise ValueError("API response must contain one or more location objects.")

            expected_keys = expected_keys or set()
            fields = {field for field in params.get("hourly", "").split(",") if field}
            for index, item in enumerate(items):
                if item.get("error") is True:
                    raise ValueError(f"API rejected location at index {index}.")
                if missing := expected_keys - item.keys():
                    raise ValueError(f"Location {index} is missing keys: {sorted(missing)}.")
                if "hourly" in expected_keys:
                    self._validate_hourly(item["hourly"], fields)
            return data
        except requests.exceptions.RequestException as error:
            status = error.response.status_code if error.response is not None else None
            logger.error("Open-Meteo request failed (%s, HTTP %s).", type(error).__name__, status)
            raise RuntimeError("Failed to fetch Open-Meteo data.") from None
        except ValueError as error:
            raise RuntimeError(f"Invalid Open-Meteo response: {error}") from None

    @staticmethod
    def _validate_hourly(hourly, fields):
        times = hourly.get("time") if isinstance(hourly, dict) else None
        if not isinstance(times, list) or not times:
            raise ValueError("The hourly time array is missing or empty.")
        for value in times:
            try:
                datetime.fromisoformat(value)
            except (TypeError, ValueError):
                raise ValueError("The hourly time array contains an invalid timestamp.") from None
        for field in (hourly.keys() | fields) - {"time"}:
            values = hourly.get(field)
            if not isinstance(values, list) or len(values) != len(times):
                raise ValueError(f"Hourly field {field} does not match the time array.")

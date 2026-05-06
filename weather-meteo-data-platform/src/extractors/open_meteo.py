import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.utils.logger import get_logger
from src.utils.config_manager import config_manager

logger = get_logger("OpenMeteoExtractor")

class OpenMeteoExtractor:
    def __init__(self, url: str):
        self.url = url
        # Tối ưu 1: Sử dụng Session để tái sử dụng TCP connection (Connection Pooling)
        self.session = requests.Session()
        
        # Tối ưu 2: Retry thông minh (Exponential Backoff) lấy từ Config
        api_config = config_manager.api_config
        retry_strategy = Retry(
            total=api_config.get("max_retries", 3),  
            status_forcelist=[429, 500, 502, 503, 504], 
            backoff_factor=api_config.get("backoff_factor", 1), 
        )
        
        # Gắn chiến lược Retry vào Session cho cả http và https
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def get_open_meteo_data(self, params: dict, expected_keys: set = None) -> dict:
        """
        Gửi request lấy dữ liệu từ Open-Meteo API sử dụng Session và Native Retry.
        """
        # Tối ưu 3: Lấy timeout từ Config
        timeout_sec = config_manager.api_config.get("timeout_sec", 10)
        
        try:
            # Dùng self.session thay vì requests.get độc lập
            response = self.session.get(self.url, params=params, timeout=timeout_sec)
            response.raise_for_status() 
            
            # Phân tách rõ quá trình lấy JSON ra khỏi quá trình lấy HTTP
            raw_data = response.json()
            
            # Tối ưu 6: Data Contract Validation (Kiểm duyệt dữ liệu đầu vào)
            if not isinstance(raw_data, dict):
                raise ValueError("API did not return a valid JSON Dictionary.")
            
            # Defensive check cho cấu trúc lỗi trả về dạng 200 OK
            if "error" in raw_data and raw_data.get("error") is True:
                reason = raw_data.get("reason", "Unknown API error")
                raise ValueError(f"API returned an error payload: {reason}")
                
            # ĐÂY MỚI LÀ DATA CONTRACT CHUẨN: 
            # Đảm bảo hình hài (schema) của cục JSON phải đúng như cam kết Business Logic.
            if expected_keys:
                missing_keys = expected_keys - raw_data.keys()
                if missing_keys:
                    raise ValueError(f"Data Contract Violation: Response is missing essential keys: {missing_keys}. Payload: {raw_data}")
                
                # Nếu có key 'hourly' trong hợp đồng, ta kiểm tra sâu hơn một chút (Nested Validation)
                if "hourly" in expected_keys and "time" not in raw_data.get("hourly", {}):
                    raise ValueError("Data Contract Violation: 'hourly' object is missing the 'time' array.")
            
            return raw_data
            
        # Tối ưu 4 (cập nhật): Bắt lỗi mạng (Network/HTTP Errors)
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP Request failed for {self.url}: {e}")
            raise RuntimeError(f"Failed to fetch data from Open-Meteo API: {e}") from e
            
        # Tối ưu 7: Bắt lỗi khi dữ liệu trả về không phải JSON hoặc vi phạm Data Contract
        except ValueError as ve:
            logger.error(f"Data Contract / JSON Decoding failed: {ve}")
            raise RuntimeError(f"Data Validation failed: {ve}") from ve
    
# Example weather & meteo usage:
if __name__ == "__main__":
    # url = "https://archive-api.open-meteo.com/v1/archive"
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
            "latitude": 10.7756, "longitude": 106.7019, # Vị trí của TP.HCM
            "current_weather": "true",
            "timezone": "Asia/Bangkok",
        }
    
    extractor = OpenMeteoExtractor(url)
    data = extractor.get_open_meteo_data(params, expected_keys={"latitude", "longitude", "hourly"})
    print(data)
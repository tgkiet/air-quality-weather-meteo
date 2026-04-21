import requests
import time
from src.utils.logger import get_logger

logger = get_logger("OpenMeteoExtractor")

class OpenMeteoExtractor:
    def __init__(self, url: str):
        self.url = url
    
    def get_open_meteo_data(self, params: dict):
        max_retries = 3
        retry_delay = 5  # seconds
        for i in range(max_retries):
            try:
                response = requests.get(self.url, params=params)
                response.raise_for_status() #check status 
                return response.json() # dua vao {}, ma python tu ep kieu tu json sang dict
            except Exception as e:
                logger.warning(f"Error fetching data: {e} (Attempt {i+1} of {max_retries})")
                time.sleep(retry_delay)
        
        logger.error(f"Failed to fetch data from {self.url} after {max_retries} attempts")
        raise Exception(f"Failed to fetch data after {max_retries} attempts")
    
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
    data = extractor.get_open_meteo_data(params)
    print(data)
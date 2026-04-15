Class OpenMeteoExtractor:
    def __init__(self, url: str):
        self.url = url
    
    def get_air_quality_data(self):
        
        
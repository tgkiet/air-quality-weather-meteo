import os
import sys
import json
import time
import requests
from datetime import datetime
from psycopg2.extras import execute_values
from psycopg2 import Error

# Ensure project root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.loaders.base_loader import BasePostgresLoader
from src.utils.logger import get_logger

logger = get_logger("HCMHistoricalBackfiller")

class HCMHistoricalBackfiller(BasePostgresLoader):
    def __init__(self):
        super().__init__()
        self.start_date = "2022-08-02"
        self.end_date = "2025-11-29"

    def run_backfill(self):
        # 1. Load config and filter HCMC locations
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "config.json")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config_data = json.load(f)
                locations = config_data["locations"]
        except Exception as e:
            logger.error(f"Failed to load config.json: {e}")
            return

        hcm_locations = [loc for loc in locations if loc["name"].startswith("HCM ")]
        logger.info(f"Found {len(hcm_locations)} HCMC locations to backfill.")

        # Connect to DB
        self.connect()

        # Loop through HCMC locations and fetch historical data
        # Assign a unique location_id offset for HCMC (3000000+)
        for idx, loc in enumerate(hcm_locations):
            location_id = 3000000 + idx
            name = loc["name"]
            lat = loc["latitude"]
            lon = loc["longitude"]

            logger.info(f"[{idx+1}/{len(hcm_locations)}] Processing {name} (lat: {lat}, lon: {lon}) | ID: {location_id}...")

            try:
                # Fetch Weather History
                weather_data = self._fetch_weather_history(lat, lon)
                time.sleep(1.0) # Be polite

                # Fetch Air Quality History
                aq_data = self._fetch_aq_history(lat, lon)
                time.sleep(1.0) # Be polite

                if not weather_data or not aq_data:
                    logger.error(f"Failed to fetch history for {name}. Skipping.")
                    continue

                # Merge and load
                self._merge_and_load(location_id, lat, lon, weather_data, aq_data)
                logger.info(f"Successfully loaded history for {name}.")

            except Exception as e:
                logger.error(f"Error backfilling {name}: {e}")
                continue

        self.close()

    def _fetch_weather_history(self, lat, lon):
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "hourly": "temperature_2m,relative_humidity_2m,precipitation,rain,wind_speed_10m,wind_direction_10m,pressure_msl",
            "timezone": "Asia/Bangkok"
        }
        for attempt in range(3):
            try:
                r = requests.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    return r.json().get("hourly", {})
                else:
                    logger.warning(f"Weather API status {r.status_code}. Attempt {attempt+1}/3. Retrying...")
            except Exception as e:
                logger.warning(f"Weather API error: {e}. Attempt {attempt+1}/3. Retrying...")
            time.sleep(2.0)
        return None

    def _fetch_aq_history(self, lat, lon):
        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone",
            "timezone": "Asia/Bangkok"
        }
        for attempt in range(3):
            try:
                r = requests.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    return r.json().get("hourly", {})
                else:
                    logger.warning(f"AQ API status {r.status_code}. Attempt {attempt+1}/3. Retrying...")
            except Exception as e:
                logger.warning(f"AQ API error: {e}. Attempt {attempt+1}/3. Retrying...")
            time.sleep(2.0)
        return None

    def _merge_and_load(self, location_id, lat, lon, weather, aq):
        # The hourly timestamps
        times = weather.get("time", [])
        if not times:
            logger.warning("No time array found in response.")
            return

        # Prepare insert tuples
        insert_rows = []
        for i, time_str in enumerate(times):
            # Parse ISO 8601 local time from Open-Meteo, convert to timezone-aware UTC base
            # Open-Meteo returns '2022-08-02T00:00'
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
            # Set to local timezone Asia/Bangkok
            # Postgres will handle local to UTC conversion automatically if inserted as TIMESTAMPTZ
            
            row = (
                time_str, # Will be parsed as TIMESTAMPTZ by Postgres
                weather["temperature_2m"][i],
                weather["relative_humidity_2m"][i],
                weather["precipitation"][i],
                weather["rain"][i],
                weather["wind_speed_10m"][i],
                weather["wind_direction_10m"][i],
                weather["pressure_msl"][i],
                aq["pm10"][i],
                aq["pm2_5"][i],
                aq["carbon_monoxide"][i],
                aq["nitrogen_dioxide"][i],
                aq["sulphur_dioxide"][i],
                aq["ozone"][i],
                location_id,
                lat,
                lon
            )
            insert_rows.append(row)

        # Batch insert using psycopg2 execute_values
        query = """
            INSERT INTO bronze_historical_weather (
                datetime, temperature_2m, relative_humidity_2m, precipitation, rain, 
                wind_speed_10m, wind_direction_10m, pressure_msl, 
                pm10_cams, pm2_5_cams, carbon_monoxide_cams, nitrogen_dioxide_cams, 
                sulphur_dioxide_cams, ozone_cams, location_id, lat, lon
            ) VALUES %s
            ON CONFLICT (datetime, lat, lon) DO UPDATE SET
                temperature_2m = EXCLUDED.temperature_2m,
                relative_humidity_2m = EXCLUDED.relative_humidity_2m,
                precipitation = EXCLUDED.precipitation,
                rain = EXCLUDED.rain,
                wind_speed_10m = EXCLUDED.wind_speed_10m,
                wind_direction_10m = EXCLUDED.wind_direction_10m,
                pressure_msl = EXCLUDED.pressure_msl,
                pm10_cams = EXCLUDED.pm10_cams,
                pm2_5_cams = EXCLUDED.pm2_5_cams,
                carbon_monoxide_cams = EXCLUDED.carbon_monoxide_cams,
                nitrogen_dioxide_cams = EXCLUDED.nitrogen_dioxide_cams,
                sulphur_dioxide_cams = EXCLUDED.sulphur_dioxide_cams,
                ozone_cams = EXCLUDED.ozone_cams,
                location_id = EXCLUDED.location_id;
        """

        try:
            with self.connection.cursor() as cursor:
                # Use template with AT TIME ZONE to handle timezone correctly
                # We tell Postgres that the incoming datetime is in Asia/Bangkok timezone
                template = "(%s AT TIME ZONE 'Asia/Bangkok', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                execute_values(cursor, query, insert_rows, template=template)
            self.connection.commit()
            logger.info(f"Inserted/Updated {len(insert_rows)} historical rows into DB.")
        except Error as e:
            self.connection.rollback()
            logger.error(f"Database error during insert: {e}")
            raise e

def main():
    backfiller = HCMHistoricalBackfiller()
    backfiller.run_backfill()

if __name__ == "__main__":
    main()

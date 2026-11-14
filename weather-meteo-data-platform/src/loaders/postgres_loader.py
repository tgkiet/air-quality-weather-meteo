from psycopg2 import extras, sql

from src.loaders.base_loader import BasePostgresLoader
from src.utils.logger import get_logger

logger = get_logger("PostgresLoader")


class PostgresLoader(BasePostgresLoader):
    def load_forecast_batch(self, execution_date, weather, air_quality):
        self._upsert_batch(
            "api_openmeteo_raw_data",
            execution_date,
            (("weather_forecast_hourly", weather), ("air_quality_hourly", air_quality)),
        )

    def insert_data(self, table_name, source_type, execution_date, raw_json):
        self._upsert_batch(table_name, execution_date, ((source_type, raw_json),))

    def _upsert_batch(self, table_name, execution_date, sources):
        if not self.connection or self.connection.closed:
            raise ConnectionError("Database connection is not established.")

        query = sql.SQL("""
            INSERT INTO {} (source_type, execution_date, raw_json)
            VALUES (%s, %s, %s)
            ON CONFLICT (source_type, execution_date) DO UPDATE SET
                raw_json = EXCLUDED.raw_json,
                ingested_at = NOW()
        """).format(sql.Identifier(table_name))
        try:
            with self.connection.cursor() as cursor:
                for source_type, payload in sources:
                    cursor.execute(query, (source_type, execution_date, extras.Json(payload)))
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

        logger.info("Stored forecast data for batch %s", execution_date)

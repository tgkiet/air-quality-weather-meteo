"""Exercise the real backfill, PostgreSQL and dbt on a disposable database."""

import json
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory

import psycopg2
from psycopg2.extras import Json

from src.scripts.backfill_history import HistoricalBackfiller
from src.scripts.data_report import COVERAGE_SQL, RAIN_SQL, TEMPERATURE_SQL


ROOT = Path(__file__).resolve().parents[1]
OLDER = "2026-01-01T00:00:00+00:00"
NEWER = "2026-01-01T01:00:00+00:00"
EARLY_HOUR = "2026-01-01T08:00"
SHARED_HOUR = "2026-01-01T09:00"


def run_dbt(workdir, command, *args):
    subprocess.run(
        [
            "dbt", "--no-use-colors", "--no-send-anonymous-usage-stats",
            command,
            "--project-dir", str(ROOT / "dbt-transform"),
            "--profiles-dir", str(ROOT / ".dbt"),
            "--target-path", str(workdir / "target"),
            "--log-path", str(workdir / "logs"),
            *args,
        ],
        check=True,
    )


def insert_forecasts(connection, batch, hours, temperature, pm25):
    with connection.cursor() as cursor:
        for source, metric, value in (
            ("weather_forecast_hourly", "temperature_2m", temperature),
            ("air_quality_hourly", "pm2_5", pm25),
        ):
            hourly = {"time": hours, metric: [value] * len(hours)}
            fetched_at = datetime.fromisoformat(batch)
            if batch == OLDER:
                # An old batch replayed after the already processed newer batch.
                fetched_at += timedelta(minutes=90)
            if source == "weather_forecast_hourly":
                hourly["precipitation"] = [3 if batch == NEWER else 1] * len(hours)
                hourly["precipitation_probability"] = [80] * len(hours)
            else:
                fetched_at += timedelta(minutes=5)
            cursor.execute(
                """INSERT INTO api_openmeteo_raw_data (source_type, execution_date, ingested_at, raw_json)
                   VALUES (%s, %s, %s, %s)""",
                (
                    source,
                    batch,
                    fetched_at,
                    Json({
                        "latitude": 21.05,
                        "longitude": 105.85,
                        "requested_latitude": 21.03,
                        "requested_longitude": 105.84,
                        "location_name": "HN Test",
                        "hourly": hourly,
                    }),
                ),
            )
    connection.commit()


def verify(connection, workdir):
    # Separate guard from the fixture write so an existing project DB is refused.
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT EXISTS (SELECT 1 FROM api_openmeteo_raw_data)
                   OR EXISTS (SELECT 1 FROM bronze_historical_weather)"""
        )
        if cursor.fetchone()[0]:
            raise RuntimeError("Use a freshly initialized disposable test database.")

    backfill = HistoricalBackfiller("2025-12-31", "2025-12-31", "HN")
    backfill.connection = connection
    backfill._merge_and_load(
        1, 21.03, 105.84, "HN Test",
        {"time": ["2025-12-31T08:00"], "temperature_2m": [17], "dew_point_2m": [11]},
        {"time": ["2025-12-31T08:00"], "pm2_5": [19], "uv_index": [3]},
    )
    insert_forecasts(connection, NEWER, [SHARED_HOUR], 27, 20)
    run_dbt(workdir, "run")

    # Simulate the two pre-upgrade Silver tables, leaving existing forecast values in place.
    with connection.cursor() as cursor:
        for table in ("slv_weather_hourly", "slv_air_quality_hourly"):
            cursor.execute(f"""ALTER TABLE silver_layer.{table}
                DROP COLUMN ingested_at, DROP COLUMN data_source,
                DROP COLUMN model_latitude, DROP COLUMN model_longitude CASCADE""")
    connection.commit()

    insert_forecasts(connection, OLDER, [EARLY_HOUR, SHARED_HOUR], 22, 42)
    for _ in range(2):
        run_dbt(workdir, "run", "--vars", json.dumps({"execution_date": OLDER}))
        verify_values(connection)

    # An archive row arrived much later for the same hour as a valid forecast.
    backfill._merge_and_load(
        1, 21.03, 105.84, "HN Test",
        {"time": [SHARED_HOUR], "temperature_2m": [15], "precipitation": [3]},
        {"time": [SHARED_HOUR], "pm2_5": [70]},
    )
    run_dbt(workdir, "run")
    verify_values(connection)
    run_dbt(workdir, "run", "--full-refresh")
    verify_values(connection)
    with connection, connection.cursor() as cursor:
        cursor.execute(
            """SELECT weather_source, air_quality_source,
                      extract(epoch FROM weather_ingested_at - air_quality_ingested_at),
                      round(air_quality_model_latitude, 2)::text,
                      round(air_quality_model_longitude, 2)::text
               FROM gold_layer.mart_hourly_conditions
               ORDER BY forecast_time"""
        )
        expected_sources = [
            ("historical", "historical", 0, None, None),
            ("forecast", "forecast", -300, "21.05", "105.85"),
            ("forecast", "forecast", -300, "21.05", "105.85"),
        ]
        sources = cursor.fetchall()
        if sources != expected_sources:
            raise AssertionError(f"Incorrect Gold provenance: {sources}")
    run_dbt(workdir, "test")
    verify_reports(connection)


def verify_values(connection):
    with connection, connection.cursor() as cursor:
        cursor.execute(
            """SELECT temperature_2m, pm2_5
               FROM gold_layer.mart_hourly_conditions
               ORDER BY forecast_time"""
        )
        values = cursor.fetchall()
        expected = [(17, 19), (22, 42), (27, 20)]
        if values != expected:
            raise AssertionError(f"Expected backfill, late row and newest forecast, got {values}")


def verify_reports(connection):
    start, end = date(2025, 12, 31), date(2026, 1, 2)
    points = ["HN Test"]
    with connection, connection.cursor() as cursor:
        cursor.execute(COVERAGE_SQL, (start, end, start, end, points, len(points)))
        coverage = cursor.fetchall()
        expected = [(start, 24, 1, 1), (date(2026, 1, 1), 24, 2, 2), (end, 24, 0, 0)]
        if coverage != expected:
            raise AssertionError(f"Incorrect coverage including empty day: {coverage}")

        cursor.execute(TEMPERATURE_SQL, (start, end, points, start, end))
        temperature = cursor.fetchall()
        if temperature != [(0, 24, 2, Decimal("9.50"), Decimal("9.50"))]:
            raise AssertionError(f"Incorrect forecast verification against archive: {temperature}")

        cursor.execute(RAIN_SQL, (start, end, points, start, end, 2, 69))
        rain = cursor.fetchall()
        if rain != [(0, 24, 2, 1, 1, 0, 0)]:
            raise AssertionError(f"Expected one rain hit and one miss against archive: {rain}")


def main():
    if os.environ.get("WAREHOUSE_TEST_DISPOSABLE_DB") != "yes":
        raise RuntimeError("This test writes fixtures. Set WAREHOUSE_TEST_DISPOSABLE_DB=yes for an isolated DB.")
    with TemporaryDirectory() as tmp:
        connection = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            dbname=os.environ["POSTGRES_DB"],
        )
        try:
            verify(connection, Path(tmp))
        finally:
            connection.close()


if __name__ == "__main__":
    main()

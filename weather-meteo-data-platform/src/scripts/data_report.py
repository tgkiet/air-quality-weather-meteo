"""Read-only data coverage and forecast verification."""
import argparse
import csv
import logging
import sys
from datetime import date, timedelta



COVERAGE_SQL = """
WITH days AS (
    SELECT day::date FROM generate_series(%s::date, %s::date, interval '1 day') AS d(day)
), counts AS (
    SELECT forecast_time_local::date AS day,
           count(DISTINCT (location_name, forecast_time)) FILTER
               (WHERE temperature_2m IS NOT NULL) AS weather_hours,
           count(DISTINCT (location_name, forecast_time)) FILTER
               (WHERE pm2_5 IS NOT NULL) AS pm25_hours
    FROM gold_layer.mart_hourly_conditions
    WHERE forecast_time_local >= %s::date
      AND forecast_time_local < %s::date + interval '1 day'
      AND location_name = ANY(%s)
    GROUP BY 1
)
SELECT days.day, %s * 24 AS expected_point_hours,
       coalesce(weather_hours, 0) AS weather_point_hours,
       coalesce(pm25_hours, 0) AS pm25_point_hours
FROM days LEFT JOIN counts USING (day)
ORDER BY day
"""

FORECAST_PAIRS_SQL = """
WITH predictions AS (
    SELECT b.ingested_at AS fetched_at,
           (t.valid_hour::timestamp AT TIME ZONE 'Asia/Bangkok') AS valid_time,
           p.point->>'location_name' AS location_name,
           round((p.point->>'requested_latitude')::numeric, 4) AS latitude,
           round((p.point->>'requested_longitude')::numeric, 4) AS longitude,
           (p.point->'hourly'->'temperature_2m'->>(t.ordinal::int - 1))::numeric AS temperature,
           (p.point->'hourly'->'precipitation'->>(t.ordinal::int - 1))::numeric AS precipitation,
           (p.point->'hourly'->'precipitation_probability'->>(t.ordinal::int - 1))::numeric AS rain_probability
    FROM public.api_openmeteo_raw_data AS b
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(b.raw_json) = 'array' THEN b.raw_json
             ELSE jsonb_build_array(b.raw_json) END
    ) AS p(point)
    CROSS JOIN LATERAL jsonb_array_elements_text(p.point->'hourly'->'time')
        WITH ORDINALITY AS t(valid_hour, ordinal)
    WHERE b.source_type = 'weather_forecast_hourly'
      AND b.execution_date >= (%s::date::timestamp AT TIME ZONE 'Asia/Bangkok') - interval '3 days'
      AND b.execution_date < (%s::date + interval '1 day') AT TIME ZONE 'Asia/Bangkok'
      AND p.point->>'location_name' = ANY(%s)
), paired AS (
    SELECT date_part('day', p.valid_time - p.fetched_at) AS lead_day,
           p.temperature - h.temperature_2m AS error,
           p.precipitation AS predicted_rain, p.rain_probability,
           h.precipitation AS reference_rain
    FROM predictions AS p
    JOIN public.bronze_historical_weather AS h
      ON h.location_name = p.location_name AND h.datetime = p.valid_time
     AND round(h.lat, 4) = p.latitude AND round(h.lon, 4) = p.longitude
    WHERE p.valid_time >= %s::date::timestamp AT TIME ZONE 'Asia/Bangkok'
      AND p.valid_time < (%s::date + interval '1 day') AT TIME ZONE 'Asia/Bangkok'
      AND p.valid_time - p.fetched_at >= interval '0 hours'
      AND p.valid_time - p.fetched_at < interval '3 days'
)
"""

TEMPERATURE_SQL = FORECAST_PAIRS_SQL + """
SELECT lead_day * 24 AS lead_hours_start, (lead_day + 1) * 24 AS lead_hours_end_exclusive,
       count(*) AS forecast_reference_pairs, round(avg(abs(error)), 2) AS mae_c,
       round(avg(error), 2) AS bias_c
FROM paired
WHERE error IS NOT NULL
GROUP BY lead_day
ORDER BY lead_day
"""


RAIN_SQL = FORECAST_PAIRS_SQL + """
, thresholds AS (
    SELECT %s::numeric AS rain_mm, %s::numeric AS rain_probability
), events AS (
    SELECT lead_day, predicted_rain >= t.rain_mm AND p.rain_probability >= t.rain_probability AS alerted,
           reference_rain >= t.rain_mm AS observed
    FROM paired AS p CROSS JOIN thresholds AS t
    WHERE predicted_rain IS NOT NULL AND p.rain_probability IS NOT NULL AND reference_rain IS NOT NULL
)
SELECT lead_day * 24 AS lead_hours_start, (lead_day + 1) * 24 AS lead_hours_end_exclusive,
       count(*) AS forecast_reference_pairs,
       count(*) FILTER (WHERE alerted AND observed) AS hits,
       count(*) FILTER (WHERE NOT alerted AND observed) AS misses,
       count(*) FILTER (WHERE alerted AND NOT observed) AS false_alarms,
       count(*) FILTER (WHERE NOT alerted AND NOT observed) AS correct_negatives
FROM events
GROUP BY lead_day
ORDER BY lead_day
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", choices=["coverage", "temperature", "rain"])
    parser.add_argument("--city", choices=["HN", "HCM"], required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    if args.end < args.start:
        parser.error("--end must be on or after --start")
    if args.report != "coverage" and args.end - args.start >= timedelta(days=31):
        parser.error("forecast verification accepts up to 31 days at a time")

    logging.disable(logging.INFO)
    from src.loaders.base_loader import BasePostgresLoader
    from src.utils.config_manager import config_manager

    names = [p["name"] for p in config_manager.locations if p["name"].startswith(args.city + " ")]
    if not names:
        parser.error("no configured points for the selected city")
    loader = BasePostgresLoader()
    try:
        connection = loader.connect()
        connection.set_session(readonly=True)
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '30s'")
            if args.report == "coverage":
                cursor.execute(COVERAGE_SQL, (args.start, args.end, args.start, args.end, names, len(names)))
            else:
                parameters = (args.start, args.end, names, args.start, args.end)
                if args.report == "temperature":
                    cursor.execute(TEMPERATURE_SQL, parameters)
                else:
                    thresholds = config_manager.alert_thresholds
                    cursor.execute(RAIN_SQL, parameters + (
                        thresholds["rain_mm"], thresholds["rain_probability_pct"],
                    ))
            writer = csv.writer(sys.stdout)
            writer.writerow(column.name for column in cursor.description)
            writer.writerows(cursor)
    finally:
        loader.close()


if __name__ == "__main__":
    main()

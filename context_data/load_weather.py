"""Loads current conditions for Toronto from Environment Canada's public
GeoMet API (no key needed) into Postgres.

Run:
  docker compose -f compose.infra.yml run --rm weather-loader
"""

import logging
import os
import sys

import psycopg2
import requests

# on-143 = the "Toronto" city-page station, checked against a handful of
# nearby stations (Toronto Island, Mississauga, Markham, ...) and picked as
# the most representative single point for the city.
STATION_URL = "https://api.weather.gc.ca/collections/citypageweather-realtime/items/on-143?f=json"

POSTGRES_DSN = (
    f"host={os.environ.get('POSTGRES_HOST', 'postgres')} "
    f"port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ.get('POSTGRES_DB', 'serving')} "
    f"user={os.environ.get('POSTGRES_USER', 'ttc')} "
    f"password={os.environ.get('POSTGRES_PASSWORD', 'ttc_dev_password')}"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("weather")

DDL = """
CREATE TABLE IF NOT EXISTS fact_weather (
    id SERIAL PRIMARY KEY,
    station_id TEXT NOT NULL,
    station_name TEXT,
    observed_at TIMESTAMPTZ NOT NULL,
    temperature_c DOUBLE PRECISION,
    condition TEXT,
    humidity_pct INTEGER,
    wind_kmh DOUBLE PRECISION,
    pressure_kpa DOUBLE PRECISION,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (station_id, observed_at)
);
"""


def parse_observation(data: dict) -> dict:
    p = data["properties"]
    cc = p["currentConditions"]
    return {
        "station_id": p["identifier"],
        "station_name": p["name"]["en"],
        "observed_at": cc["timestamp"]["en"],
        "temperature_c": cc.get("temperature", {}).get("value", {}).get("en"),
        "condition": cc.get("condition", {}).get("en"),
        "humidity_pct": cc.get("relativeHumidity", {}).get("value", {}).get("en"),
        "wind_kmh": cc.get("wind", {}).get("speed", {}).get("value", {}).get("en"),
        "pressure_kpa": cc.get("pressure", {}).get("value", {}).get("en"),
    }


def main():
    log.info(f"Fetching {STATION_URL}")
    resp = requests.get(STATION_URL, timeout=30)
    resp.raise_for_status()
    obs = parse_observation(resp.json())
    log.info(f"{obs['station_name']} at {obs['observed_at']}: {obs['temperature_c']}C, {obs['condition']}")

    conn = psycopg2.connect(POSTGRES_DSN)
    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute(
            """
            INSERT INTO fact_weather
                (station_id, station_name, observed_at, temperature_c, condition, humidity_pct, wind_kmh, pressure_kpa)
            VALUES (%(station_id)s, %(station_name)s, %(observed_at)s, %(temperature_c)s,
                    %(condition)s, %(humidity_pct)s, %(wind_kmh)s, %(pressure_kpa)s)
            ON CONFLICT (station_id, observed_at) DO NOTHING
            """,
            obs,
        )
    conn.commit()
    conn.close()
    log.info("Done")


if __name__ == "__main__":
    main()

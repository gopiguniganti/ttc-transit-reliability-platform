"""Loads Toronto's road restrictions (closures/construction) feed, then
spatial-joins it against dim_stop to find which stops are near an active
restriction -- no PostGIS installed on this Postgres, so the join is a
plain-Python haversine distance calc instead of a real GIS query.

Run:
  docker compose -f compose.infra.yml run --rm road-restrictions-loader
"""

import json
import logging
import math
import os
import re
import sys

import psycopg2
import requests

RESTRICTIONS_URL = "https://secure.toronto.ca/opendata/cart/road_restrictions/v3?format=json"
NEARBY_RADIUS_M = 150  # "affects this stop" threshold -- roughly a block

POSTGRES_DSN = (
    f"host={os.environ.get('POSTGRES_HOST', 'postgres')} "
    f"port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ.get('POSTGRES_DB', 'serving')} "
    f"user={os.environ.get('POSTGRES_USER', 'ttc')} "
    f"password={os.environ.get('POSTGRES_PASSWORD', 'ttc_dev_password')}"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("road_restrictions")

DDL = """
CREATE TABLE IF NOT EXISTS road_restrictions (
    restriction_id TEXT PRIMARY KEY,
    road TEXT,
    road_class TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    planned BOOLEAN,
    expired BOOLEAN,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    description TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stops_near_restrictions (
    stop_id TEXT NOT NULL,
    restriction_id TEXT NOT NULL,
    distance_m DOUBLE PRECISION NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stop_id, restriction_id)
);
"""


def fetch_restrictions(url: str) -> list[dict]:
    """Toronto's feed contains at least one unescaped literal backslash
    (seen in the wild: `"description":"...Water \\ Sewer"`), which is
    invalid JSON -- a backslash must be followed by a valid escape
    character. Rather than fail outright, escape any backslash that
    ISN'T already starting a valid escape sequence."""
    raw = requests.get(url, timeout=60).text
    fixed = re.sub(r'\\(?!["\\/bfnrtu])', r"\\\\", raw)
    return json.loads(fixed)["Closure"]


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000  # Earth's radius, meters
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def grid_key(lat: float, lon: float) -> tuple:
    # ~0.001 degrees of latitude is ~111m -- rounding to 3 decimals buckets
    # points into ~111m x ~111m cells, a cheap stand-in for a real spatial
    # index (an R-tree, or PostGIS's GiST index) when neither is available.
    return (round(lat, 3), round(lon, 3))


def find_nearby_stops(stops: list[dict], restrictions: list[dict], radius_m: float) -> list[dict]:
    """For each restriction, finds stops within radius_m. Grid-bucketed so
    this doesn't have to compare every stop against every restriction
    (9,361 x 2,187 = ~20M pairs full cross product) -- only stops sharing
    or adjacent to a restriction's grid cell are checked with the real
    haversine formula."""
    buckets: dict[tuple, list[dict]] = {}
    for s in stops:
        buckets.setdefault(grid_key(s["lat"], s["lon"]), []).append(s)

    results = []
    for r in restrictions:
        rk = grid_key(r["lat"], r["lon"])
        candidates = []
        for dlat in (-1, 0, 1):
            for dlon in (-1, 0, 1):
                candidates.extend(buckets.get((round(rk[0] + dlat * 0.001, 3), round(rk[1] + dlon * 0.001, 3)), []))
        for s in candidates:
            d = haversine_m(r["lat"], r["lon"], s["lat"], s["lon"])
            if d <= radius_m:
                results.append({"stop_id": s["stop_id"], "restriction_id": r["restriction_id"], "distance_m": d})
    return results


def epoch_ms_to_ts(v):
    return int(v) / 1000 if v else None


def main():
    log.info(f"Fetching {RESTRICTIONS_URL}")
    closures = fetch_restrictions(RESTRICTIONS_URL)
    log.info(f"Parsed {len(closures)} road restrictions")

    conn = psycopg2.connect(POSTGRES_DSN)
    with conn.cursor() as cur:
        cur.execute(DDL)

        cur.execute("TRUNCATE road_restrictions, stops_near_restrictions")
        for c in closures:
            cur.execute(
                """
                INSERT INTO road_restrictions
                    (restriction_id, road, road_class, latitude, longitude, planned, expired,
                     start_time, end_time, description)
                VALUES (%s, %s, %s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s), %s)
                """,
                (
                    c["id"], c.get("road"), c.get("roadClass"),
                    float(c["latitude"]) if c.get("latitude") else None,
                    float(c["longitude"]) if c.get("longitude") else None,
                    bool(c.get("planned")), bool(c.get("expired")),
                    epoch_ms_to_ts(c.get("startTime")), epoch_ms_to_ts(c.get("endTime")),
                    c.get("description"),
                ),
            )
        conn.commit()
        log.info(f"road_restrictions: {len(closures)} loaded")

        cur.execute("SELECT stop_id, stop_lat, stop_lon FROM dim_stop WHERE is_current AND stop_lat IS NOT NULL")
        stops = [{"stop_id": r[0], "lat": r[1], "lon": r[2]} for r in cur.fetchall()]

    restrictions = [
        {"restriction_id": c["id"], "lat": float(c["latitude"]), "lon": float(c["longitude"])}
        for c in closures if c.get("latitude") and c.get("longitude")
    ]
    log.info(f"Spatial join: {len(stops)} stops vs {len(restrictions)} located restrictions, radius={NEARBY_RADIUS_M}m")

    nearby = find_nearby_stops(stops, restrictions, NEARBY_RADIUS_M)
    log.info(f"Found {len(nearby)} (stop, restriction) pairs within {NEARBY_RADIUS_M}m")

    with conn.cursor() as cur:
        for pair in nearby:
            cur.execute(
                "INSERT INTO stops_near_restrictions (stop_id, restriction_id, distance_m) VALUES (%s, %s, %s)",
                (pair["stop_id"], pair["restriction_id"], pair["distance_m"]),
            )
    conn.commit()
    conn.close()
    log.info("Done")


if __name__ == "__main__":
    main()

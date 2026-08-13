"""Polls TTC's GTFS-RT feed, writes raw protobuf to disk, decodes to Parquet,
and optionally publishes to Kafka. Raw bytes are kept as source of truth so
decode/write bugs can be fixed and replayed without re-fetching the feed."""

import gzip
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.transit import gtfs_realtime_pb2

try:
    from confluent_kafka import Producer
except ImportError:
    Producer = None  # optional dep, only needed if KAFKA_BOOTSTRAP_SERVERS is set

FEED_URL = os.environ.get("POLL_URL", "https://bustime.ttc.ca/gtfsrt/vehicles")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "20"))
FLUSH_INTERVAL_SECONDS = int(os.environ.get("FLUSH_INTERVAL_SECONDS", "300"))  # 5 min
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
RAW_DIR = DATA_DIR / "raw" / "vehicle_positions"
BRONZE_DIR = DATA_DIR / "bronze" / "vehicle_positions"
REQUEST_TIMEOUT_SECONDS = 15
MAX_CONSECUTIVE_FAILURES = 10

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")  # empty = Kafka publish disabled
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ttc.vehicle_positions")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("poller")

SCHEMA = pa.schema([
    ("feed_timestamp", pa.int64()),      # when TTC generated this snapshot (epoch seconds)
    ("ingest_timestamp", pa.int64()),    # when WE received it (epoch seconds) -- these differ!
    ("entity_id", pa.string()),
    ("vehicle_id", pa.string()),
    ("trip_id", pa.string()),
    ("route_id", pa.string()),
    ("direction_id", pa.int32()),
    ("latitude", pa.float32()),
    ("longitude", pa.float32()),
    ("bearing", pa.float32()),
    ("speed", pa.float32()),
    ("current_stop_sequence", pa.int32()),
    ("stop_id", pa.string()),
    ("current_status", pa.string()),     # INCOMING_AT / STOPPED_AT / IN_TRANSIT_TO
    ("vehicle_timestamp", pa.int64()),   # when the VEHICLE reported this position
    ("occupancy_status", pa.string()),
])


class GracefulShutdown:
    """Catches SIGTERM/SIGINT so the buffer gets flushed before exit instead
    of losing up to FLUSH_INTERVAL_SECONDS of data on every restart."""
    def __init__(self):
        self.shutdown = False
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum, frame):
        log.info(f"Received signal {signum}, will flush and exit after this cycle")
        self.shutdown = True


def fetch_feed() -> bytes:
    """Fetch raw protobuf bytes. Raises on non-200 or network error."""
    resp = requests.get(FEED_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return resp.content


def save_raw(raw_bytes: bytes, ts: datetime) -> None:
    """Persist the untouched bytes, gzip-compressed, partitioned by date/hour."""
    partition_dir = RAW_DIR / f"date={ts:%Y-%m-%d}" / f"hour={ts:%H}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{ts:%Y%m%dT%H%M%S}.pb.gz"
    with gzip.open(partition_dir / filename, "wb") as f:
        f.write(raw_bytes)


def decode_entities(raw_bytes: bytes, ingest_ts: int) -> list[dict]:
    """Turn protobuf bytes into a list of dicts, one per vehicle. Most
    GTFS-RT fields are optional, hence the HasField() checks throughout."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_bytes)

    rows = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle

        row = {
            "feed_timestamp": feed.header.timestamp,
            "ingest_timestamp": ingest_ts,
            "entity_id": entity.id,
            "vehicle_id": v.vehicle.id if v.HasField("vehicle") else None,
            "trip_id": v.trip.trip_id if v.HasField("trip") else None,
            "route_id": v.trip.route_id if v.HasField("trip") else None,
            "direction_id": v.trip.direction_id if v.HasField("trip") and v.trip.HasField("direction_id") else None,
            "latitude": v.position.latitude if v.HasField("position") else None,
            "longitude": v.position.longitude if v.HasField("position") else None,
            "bearing": v.position.bearing if v.HasField("position") and v.position.HasField("bearing") else None,
            "speed": v.position.speed if v.HasField("position") and v.position.HasField("speed") else None,
            "current_stop_sequence": v.current_stop_sequence if v.HasField("current_stop_sequence") else None,
            "stop_id": v.stop_id if v.HasField("stop_id") else None,
            "current_status": _status_name(v) if v.HasField("current_status") else None,
            "vehicle_timestamp": v.timestamp if v.HasField("timestamp") else None,
            "occupancy_status": _occupancy_name(v) if v.HasField("occupancy_status") else None,
        }
        rows.append(row)
    return rows


def _status_name(v) -> str:
    return gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(v.current_status)


def _occupancy_name(v) -> str:
    return gtfs_realtime_pb2.VehiclePosition.OccupancyStatus.Name(v.occupancy_status)


def flush_to_parquet(buffer: list[dict], ts: datetime) -> None:
    """Write buffered rows as one Parquet file using the explicit SCHEMA."""
    if not buffer:
        return
    partition_dir = BRONZE_DIR / f"date={ts:%Y-%m-%d}" / f"hour={ts:%H}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{ts:%Y%m%dT%H%M%S}.parquet"

    table = pa.Table.from_pylist(buffer, schema=SCHEMA)
    pq.write_table(table, partition_dir / filename, compression="snappy")
    log.info(f"Flushed {len(buffer)} rows -> {partition_dir / filename}")


def build_kafka_producer():
    """Returns a Producer if KAFKA_BOOTSTRAP_SERVERS is set, else None."""
    if not KAFKA_BOOTSTRAP_SERVERS:
        return None
    if Producer is None:
        log.warning("KAFKA_BOOTSTRAP_SERVERS is set but confluent-kafka isn't installed; skipping Kafka publish")
        return None
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def publish_rows(producer, rows: list[dict], topic: str = KAFKA_TOPIC) -> None:
    """Publish one JSON message per row, keyed by vehicle_id so a single
    vehicle's updates stay ordered within a partition. Never raises -- a
    Kafka outage shouldn't take down the poller."""
    if producer is None or not rows:
        return
    try:
        for row in rows:
            key = (row.get("vehicle_id") or "").encode("utf-8")
            producer.produce(topic, key=key, value=json.dumps(row).encode("utf-8"))
        producer.poll(0)
    except Exception as e:
        log.error(f"Kafka publish failed (continuing without it): {e}")


def main():
    shutdown = GracefulShutdown()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    log.info(f"Starting poller: url={FEED_URL} interval={POLL_INTERVAL_SECONDS}s flush={FLUSH_INTERVAL_SECONDS}s")

    kafka_producer = build_kafka_producer()
    log.info(f"Kafka publish: {'ENABLED -> ' + KAFKA_TOPIC if kafka_producer else 'disabled'}")

    buffer: list[dict] = []
    last_flush = time.monotonic()
    consecutive_failures = 0

    while not shutdown.shutdown:
        cycle_start = time.monotonic()
        now = datetime.now(timezone.utc)

        try:
            raw_bytes = fetch_feed()
            save_raw(raw_bytes, now)
            rows = decode_entities(raw_bytes, int(now.timestamp()))
            buffer.extend(rows)
            publish_rows(kafka_producer, rows)
            consecutive_failures = 0
            log.info(f"Polled OK: {len(rows)} vehicles, buffer={len(buffer)}")
        except Exception as e:
            consecutive_failures += 1
            log.error(f"Poll failed ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}): {e}")
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                log.error("Too many consecutive failures, flushing what we have and exiting")
                flush_to_parquet(buffer, now)
                if kafka_producer:
                    kafka_producer.flush(timeout=10)
                sys.exit(1)

        if time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS:
            flush_to_parquet(buffer, now)
            buffer = []
            last_flush = time.monotonic()

        elapsed = time.monotonic() - cycle_start
        sleep_for = max(0, POLL_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_for)

    log.info("Shutting down, flushing remaining buffer")
    flush_to_parquet(buffer, datetime.now(timezone.utc))
    if kafka_producer:
        kafka_producer.flush(timeout=10)


if __name__ == "__main__":
    main()

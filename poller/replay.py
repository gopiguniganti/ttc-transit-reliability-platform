"""Republishes archived raw .pb.gz files to Kafka, using poller.py's own
decode/publish logic. Useful for backfilling a Kafka consumer or testing
against real-shaped data without hitting the live feed.

Usage:
  python3 replay.py --dir /path/to/raw/vehicle_positions \\
      --kafka localhost:9092 --speed asap

  --speed asap       publish everything back-to-back
  --speed realtime   pace playback to match the original poll gaps
"""
import argparse
import gzip
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import poller  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("replay")


def find_raw_files(root: Path) -> list[Path]:
    """Filenames are {ts:%Y%m%dT%H%M%S}.pb.gz, so sorting by name is chronological."""
    return sorted(root.rglob("*.pb.gz"))


def file_timestamp(path: Path) -> datetime:
    # path.stem only strips one suffix, leaving ".pb" behind -- strip the
    # full ".pb.gz" explicitly instead.
    return datetime.strptime(path.name.removesuffix(".pb.gz"), "%Y%m%dT%H%M%S")


def replay(root: Path, producer, topic: str, speed: str) -> None:
    files = find_raw_files(root)
    if not files:
        log.warning(f"No .pb.gz files found under {root}")
        return
    log.info(f"Replaying {len(files)} files from {root} (speed={speed}) -> topic={topic}")

    prev_ts = None
    total_rows = 0
    for i, path in enumerate(files):
        if speed == "realtime" and prev_ts is not None:
            gap = (file_timestamp(path) - prev_ts).total_seconds()
            if gap > 0:
                time.sleep(gap)
        prev_ts = file_timestamp(path)

        with gzip.open(path, "rb") as f:
            raw_bytes = f.read()
        rows = poller.decode_entities(raw_bytes, ingest_ts=int(prev_ts.timestamp()))
        poller.publish_rows(producer, rows, topic=topic)
        total_rows += len(rows)

        if (i + 1) % 50 == 0 or i == len(files) - 1:
            log.info(f"Replayed {i + 1}/{len(files)} files, {total_rows} rows published so far")

    if producer:
        producer.flush(timeout=30)
    log.info(f"Done. {total_rows} total rows published from {len(files)} files.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir", required=True, type=Path, help="Root of raw/vehicle_positions to replay")
    parser.add_argument("--kafka", required=True, help="Kafka bootstrap servers, e.g. localhost:9092")
    parser.add_argument("--topic", default="ttc.vehicle_positions")
    parser.add_argument("--speed", choices=["asap", "realtime"], default="asap")
    args = parser.parse_args()

    poller.KAFKA_BOOTSTRAP_SERVERS = args.kafka  # build_kafka_producer() reads this module-level var
    producer = poller.build_kafka_producer()
    if producer is None:
        log.error("Could not build a Kafka producer (is confluent-kafka installed? is --kafka reachable?)")
        sys.exit(1)

    replay(args.dir, producer, args.topic, args.speed)


if __name__ == "__main__":
    main()

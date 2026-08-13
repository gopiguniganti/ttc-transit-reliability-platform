"""Decode logic tested against a hand-built FeedMessage, never the live
feed -- the live feed isn't reproducible, so it can't be asserted against."""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "poller"))

from google.transit import gtfs_realtime_pb2
import poller


def build_synthetic_feed():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1700000000

    full = feed.entity.add()
    full.id = "full-vehicle"
    full.vehicle.vehicle.id = "1234"
    full.vehicle.trip.trip_id = "trip_001"
    full.vehicle.trip.route_id = "504"
    full.vehicle.trip.direction_id = 0
    full.vehicle.position.latitude = 43.6532
    full.vehicle.position.longitude = -79.3832
    full.vehicle.position.bearing = 90.0
    full.vehicle.position.speed = 8.3
    full.vehicle.current_stop_sequence = 12
    full.vehicle.stop_id = "stop_555"
    full.vehicle.current_status = gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO
    full.vehicle.timestamp = 1700000000
    full.vehicle.occupancy_status = gtfs_realtime_pb2.VehiclePosition.MANY_SEATS_AVAILABLE

    sparse = feed.entity.add()
    sparse.id = "sparse-vehicle"
    sparse.vehicle.vehicle.id = "5678"
    sparse.vehicle.position.latitude = 43.66
    sparse.vehicle.position.longitude = -79.39
    # deliberately no trip, no bearing, no status, no occupancy

    return feed.SerializeToString()


def test_decode_full_vehicle_extracts_all_fields():
    raw = build_synthetic_feed()
    rows = poller.decode_entities(raw, ingest_ts=1700000005)

    full = next(r for r in rows if r["entity_id"] == "full-vehicle")
    assert full["route_id"] == "504"
    assert full["vehicle_id"] == "1234"
    assert round(full["latitude"], 4) == 43.6532
    assert full["current_status"] == "IN_TRANSIT_TO"
    assert full["occupancy_status"] == "MANY_SEATS_AVAILABLE"
    assert full["feed_timestamp"] == 1700000000
    assert full["ingest_timestamp"] == 1700000005


def test_decode_sparse_vehicle_does_not_crash_and_nulls_missing_fields():
    raw = build_synthetic_feed()
    rows = poller.decode_entities(raw, ingest_ts=1700000005)

    sparse = next(r for r in rows if r["entity_id"] == "sparse-vehicle")
    assert sparse["vehicle_id"] == "5678"
    assert sparse["route_id"] is None
    assert sparse["trip_id"] is None
    assert sparse["current_status"] is None
    assert sparse["bearing"] is None


def test_decode_returns_one_row_per_vehicle_entity():
    raw = build_synthetic_feed()
    rows = poller.decode_entities(raw, ingest_ts=1700000005)
    assert len(rows) == 2


def test_flush_to_parquet_writes_readable_file(tmp_path, monkeypatch):
    import pyarrow.parquet as pq
    from datetime import datetime, timezone

    monkeypatch.setattr(poller, "BRONZE_DIR", tmp_path / "bronze")

    raw = build_synthetic_feed()
    rows = poller.decode_entities(raw, ingest_ts=1700000005)
    now = datetime.now(timezone.utc)

    poller.flush_to_parquet(rows, now)

    files = list((tmp_path / "bronze").rglob("*.parquet"))
    assert len(files) == 1

    # pq.read_table() auto-detects Hive partitioning from parent dir names
    # and injects phantom date/hour columns; ParquetFile reads only the
    # file's actual columns.
    table = pq.ParquetFile(files[0]).read()
    assert table.num_rows == 2
    assert set(table.column_names) == set(c.name for c in poller.SCHEMA)


def test_build_kafka_producer_returns_none_when_disabled(monkeypatch):
    monkeypatch.setattr(poller, "KAFKA_BOOTSTRAP_SERVERS", "")
    assert poller.build_kafka_producer() is None


def test_publish_rows_is_a_noop_without_a_producer():
    poller.publish_rows(None, [{"vehicle_id": "1234"}])


class FakeProducer:
    """Stands in for confluent_kafka.Producer, no real broker needed."""
    def __init__(self):
        self.produced = []

    def produce(self, topic, key, value):
        self.produced.append((topic, key, value))

    def poll(self, timeout):
        pass


def test_publish_rows_sends_one_json_message_per_row_keyed_by_vehicle_id():
    producer = FakeProducer()
    rows = [
        {"vehicle_id": "1234", "route_id": "504"},
        {"vehicle_id": "5678", "route_id": "501"},
    ]
    poller.publish_rows(producer, rows, topic="ttc.vehicle_positions")

    assert len(producer.produced) == 2
    topic, key, value = producer.produced[0]
    assert topic == "ttc.vehicle_positions"
    assert key == b"1234"
    assert json.loads(value) == rows[0]


def test_publish_rows_swallows_kafka_errors():
    class BrokenProducer:
        def produce(self, *a, **kw):
            raise RuntimeError("broker unreachable")

    poller.publish_rows(BrokenProducer(), [{"vehicle_id": "1234"}])

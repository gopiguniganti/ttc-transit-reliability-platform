# Toronto Transit Reliability Platform

A learning project: a lakehouse pipeline that ingests real TTC realtime
vehicle positions (plus static schedules, road restrictions, and weather —
coming in later phases) to answer *where, when, and why TTC surface
service is unreliable.*

Status: **Phase 1 done, Phase 2 (Kafka + replay) built but not yet deployed
to the live poller.** See [`docs/architecture.md`](docs/architecture.md) for
the full architecture and decisions log.

## Tonight: deploy the poller on your always-on machine

This does ONE thing: polls TTC's GTFS-RT feed every 20s, saves the raw
Protobuf bytes (ground truth, replayable later), and writes batched Parquet
files every 5 minutes. It needs ~200MB RAM and no GPU. Nothing else in this
repo runs tonight.

```bash
git clone <this-repo>
cd ttc-platform
make poller-up
make poller-logs        # watch it start pulling data; Ctrl+C to stop watching (poller keeps running)
```

Check on it any time:
```bash
make poller-status
```

Stop it whenever (data already on disk is untouched):
```bash
make poller-down
```

## Run the tests (no network needed, works anywhere)

```bash
pip install pytest gtfs-realtime-bindings pyarrow --break-system-packages
make test
```

## What's on disk after a night of collection

```
data/
├── raw/vehicle_positions/date=YYYY-MM-DD/hour=HH/*.pb.gz     <- ground truth
└── bronze/vehicle_positions/date=YYYY-MM-DD/hour=HH/*.parquet <- decoded, queryable
```

## Why it's built this way (short version — full decisions doc later)

- **Raw bytes saved before decoding.** GTFS-RT publishes no history — this
  minute's data is gone forever once TTC's server moves on. If our decode
  logic has a bug, we fix it and replay the raw files; we can't ask TTC to
  resend last Tuesday.
- **Batched flush (5 min), not one file per poll (20s).** One-file-per-poll
  would be 4,320 tiny files/day. Small files carry fixed per-file overhead
  that dominates read performance at scale — this is the single most common
  real-world data engineering performance problem.
- **Explicit Parquet schema, not "infer it later."** Every file has
  identical column types by construction, which matters once many files get
  read as one logical table by Spark/DuckDB downstream.
- **date=/hour= partition folders.** Lets a query engine skip whole folders
  when filtering by date (partition pruning), and lets us delete/archive old
  data by removing a folder.
- **Tested against a synthetic message, not the live feed.** The live feed
  is different every time you call it — not reproducible, not a real test.
  A hand-built protobuf message with known values is.

## Phase 2: Kafka + replay mode

The poller can now optionally publish each decoded vehicle row to Kafka, one
JSON message per vehicle, keyed by `vehicle_id`, in ADDITION to (never
instead of) the existing raw + bronze Parquet writes. It's off by default --
set `KAFKA_BOOTSTRAP_SERVERS` to turn it on, e.g. in `compose.poller.yml`:

```yaml
environment:
  KAFKA_BOOTSTRAP_SERVERS: "192.168.2.40:9092"   # Beast's LAN IP
```

**This is not yet turned on for the live poller on prodesk** -- it needs the
updated `poller/` code deployed there first. This repo is now on GitHub
(https://github.com/gopiguniganti/ttc-transit-reliability-platform), but
prodesk hasn't been set up to `git pull` from it yet -- see CLAUDE.md.

**Replay tool** (`poller/replay.py`) republishes already-collected raw
`.pb.gz` files to Kafka -- useful for testing a consumer against real-shaped
data without touching the live TTC feed:

```bash
make replay-asap       # publish everything back-to-back
make replay-realtime   # paced to match the original ~20s poll gaps
```

## Roadmap

- [x] Phase 1: raw + bronze Parquet collection
- [~] Phase 2: Kafka (real-time layer) + replay mode -- code done (poller's optional
      Kafka publish path, `poller/replay.py`), NOT yet deployed to the live poller on
      prodesk. See "Phase 2" section below.
- [ ] Phase 3: GTFS static → dimensional model + SCD2
- [ ] Phase 4: Spark Structured Streaming → Delta Lake
- [ ] Phase 5: silver layer, headway/bunching detection
- [ ] Phase 6: road restrictions (spatial join) + weather
- [ ] Phase 7: Airflow orchestration
- [ ] Phase 8: dbt models + tests
- [ ] Phase 9: Grafana, CI, decisions doc, packaging

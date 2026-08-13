# Toronto Transit Reliability Platform

A learning project: a lakehouse pipeline that ingests real TTC realtime vehicle positions, plus static
schedules, road restrictions, and weather, to answer *where, when, and why TTC surface service is
unreliable.*

Status: **Phases 1–7 built and verified end to end** — live ingestion, Kafka, Structured Streaming to
Delta, a dimensional model, a bunching heuristic, weather/road-restriction context data, and Airflow
orchestration for all of it. See [`docs/architecture.md`](docs/architecture.md) for the full
architecture, every design decision, and the real bugs found while building it (with numbers, not just
"it works").

This runs across two machines — see the "Two machines, two jobs" section in
[`docs/architecture.md`](docs/architecture.md) for why:

- **collector host** — an always-on machine that just runs the poller (`compose.poller.yml`)
- **compute host** — your PC, running everything else (`compose.infra.yml`): Kafka, MinIO, Postgres,
  Spark, Jupyter, Airflow

## Quickstart

### On the collector host: run the poller

```bash
git clone <this-repo>
cd ttc-platform
make poller-up
make poller-logs        # watch it start pulling data; Ctrl+C to stop watching (poller keeps running)
```

Check on it any time, or stop it (data already on disk is untouched either way):
```bash
make poller-status
make poller-down
```

### On the compute host: run everything else

```bash
make on          # start Kafka, MinIO, Postgres, Spark, Jupyter, Airflow, pgAdmin
make infra-status # service URLs + credentials
make off          # stop everything before shutting the machine down; no data is lost
```

`make on`/`make off` are just short names for `infra-up`/`infra-down` — handy for an overnight routine
if you don't want the compute host running 24/7 (the collector host is the only piece that needs to be).

### Everything else is on-demand, via `make`

```bash
make gtfs-load          # load routes/stops/trips from TTC's static GTFS feed (Phase 3)
make weather-load        # current Toronto conditions (Phase 6)
make restrictions-load   # road restrictions + spatial join against stops (Phase 6)
make replay-asap          # republish collected raw files into Kafka, for testing (Phase 2)
```

All four also have an Airflow DAG that runs them on a schedule instead — paused by default, see
Phase 7 in the architecture doc.

## Run the tests (no network needed, works anywhere)

Each Python subproject has its own tests, run inside its own Docker image so dependencies match exactly
what production uses:

```bash
make test   # poller + replay tests
```

The same pattern applies to `gtfs_static/`, `context_data/`, and their tests under `tests/` — build that
subproject's image and run pytest inside it (see `docs/architecture.md` for the exact commands used
while building each phase).

## What's on disk after a night of collection

```
data/
├── raw/vehicle_positions/date=YYYY-MM-DD/hour=HH/*.pb.gz     <- ground truth
└── bronze/vehicle_positions/date=YYYY-MM-DD/hour=HH/*.parquet <- decoded, queryable
```

Plus, once the compute host's stack has run: a Delta table on MinIO fed by Kafka in real time, a
dimensional model and bunching/weather/restriction tables in Postgres, and Airflow DAGs ready to keep
all of it fresh on a schedule.

## Why it's built this way (short version — full decisions log in `docs/architecture.md`)

- **Raw bytes saved before decoding.** GTFS-RT publishes no history — this minute's data is gone
  forever once TTC's server moves on. If decode logic ever has a bug, the fix gets applied and the raw
  files get replayed to regenerate correct output.
- **Batched flush, not one file per poll.** Avoids the classic small-files performance problem.
- **Explicit schemas everywhere**, not "infer it later" — Parquet, Kafka JSON, and the Postgres
  dimensional model all declare their shape up front.
- **Raw is the source of truth, everything else is derived and replayable** — Kafka, Delta, and the
  Postgres tables can all be rebuilt from the raw archive if something downstream needs fixing.
- **Use the right tool for the data size**, not the most impressive one — GTFS static, weather, and
  road restrictions are all small enough for plain Python; Spark is reserved for the live position
  stream and the bunching analysis, which actually need it.

## Roadmap

- [x] Phase 1: raw + bronze Parquet collection
- [x] Phase 2: Kafka (real-time layer) + replay mode
- [x] Phase 3: GTFS static → dimensional model + SCD2
- [x] Phase 4: Spark Structured Streaming → Delta Lake
- [x] Phase 5: silver layer, bunching heuristic
- [x] Phase 6: road restrictions (spatial join) + weather
- [x] Phase 7: Airflow orchestration
- [ ] Phase 8: dbt models + tests
- [ ] Phase 9: Grafana, CI, decisions doc, packaging

.PHONY: help poller-up poller-down poller-logs poller-status test infra-up infra-down infra-status checkpoint kafka-topics notebook-logs replay-asap replay-realtime stream-logs on off gtfs-load weather-load restrictions-load

help:
	@echo "--- poller (homelab) ---"
	@echo "poller-up      Build and start the poller (run this on the homelab tonight)"
	@echo "poller-down    Stop the poller (safe -- data stays on disk)"
	@echo "poller-logs    Follow poller logs"
	@echo "poller-status  Show container status + how much data has landed"
	@echo "test           Run unit tests against the poller's decode logic"
	@echo "replay-asap    Replay collected raw files into Kafka, as fast as possible"
	@echo "replay-realtime  Same, but paced to match the original poll timing"
	@echo ""
	@echo "--- infra (PC) ---"
	@echo "on             Alias for infra-up -- run this after powering the PC back on"
	@echo "off            Alias for infra-down -- run this before shutting the PC down"
	@echo "infra-up       Start Kafka, MinIO, Postgres, Spark cluster"
	@echo "infra-down     Stop everything (data persists in Docker volumes)"
	@echo "infra-status   Show container status + service URLs"
	@echo "kafka-topics   Create the Kafka topics we need"
	@echo "checkpoint     Run the Spark<->MinIO<->Delta connectivity test"
	@echo "notebook-logs  Follow JupyterLab logs (URL also shown by infra-status)"
	@echo "stream-logs    Follow the Kafka->Delta streaming job's logs"
	@echo "gtfs-load      Load routes/stops/trips from TTC's GTFS static feed into Postgres"
	@echo "weather-load   Load current Toronto conditions from Environment Canada"
	@echo "restrictions-load  Load road restrictions + spatial-join against stops"
	@echo "dbt-build      Build the dbt staging/mart models over Postgres's serving DB"
	@echo "dbt-test       Run dbt's data tests (not_null/unique/etc, see dbt/models)"
	@echo "dbt-docs       Generate + serve dbt docs at http://localhost:8087"

poller-up:
	docker compose -f compose.poller.yml up -d --build

poller-down:
	docker compose -f compose.poller.yml down

poller-logs:
	docker compose -f compose.poller.yml logs -f --tail=50

poller-status:
	@docker compose -f compose.poller.yml ps
	@echo ""
	@echo "Raw files:"
	@find ./data/raw -name '*.pb.gz' 2>/dev/null | wc -l
	@echo "Parquet files:"
	@find ./data/bronze -name '*.parquet' 2>/dev/null | wc -l
	@echo "Disk used:"
	@du -sh ./data 2>/dev/null || echo "no data yet"

test:
	cd poller && python3 -m pytest ../tests/ -v

infra-up:
	docker compose -f compose.infra.yml up -d
	@echo ""
	@echo "Waiting ~15s for services to become healthy..."
	@sleep 15
	@$(MAKE) infra-status

infra-down:
	docker compose -f compose.infra.yml down

on: infra-up
off: infra-down

infra-status:
	@docker compose -f compose.infra.yml ps
	@echo ""
	@echo "Spark master UI:  http://localhost:8080  (cluster/worker health only -- no jobs/DAGs)"
	@echo "Spark App UI:     http://localhost:4040  (Jobs/Stages/DAG -- LIVE, only while a Jupyter job runs)"
	@echo "Spark App UI:     http://localhost:4041  (same, for jobs run via 'make checkpoint')"
	@echo "Spark History:    http://localhost:18080 (Jobs/Stages/DAG -- PERSISTED, after jobs finish)"
	@echo "Kafka UI (AKHQ):  http://localhost:8085"
	@echo "MinIO console:    http://localhost:9001  (minioadmin / minioadmin123)"
	@echo "Postgres:         localhost:5433  (ttc / ttc_dev_password, db=serving)"
	@echo "pgAdmin:          http://localhost:5050  (admin@ttcplatform.dev / admin)"
	@echo "JupyterLab:       http://localhost:8888  (no token)"
	@echo "Streaming job UI: http://localhost:4042  (Kafka -> Delta, live while the query runs)"

notebook-logs:
	docker compose -f compose.infra.yml logs -f --tail=50 jupyter

stream-logs:
	docker compose -f compose.infra.yml logs -f --tail=50 spark-streaming

kafka-topics:
	docker exec ttc-kafka /opt/kafka/bin/kafka-topics.sh --create \
		--bootstrap-server localhost:9092 \
		--topic ttc.vehicle_positions --partitions 6 --replication-factor 1 \
		--config retention.ms=604800000 || true
	docker exec ttc-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Reads ./data/raw/vehicle_positions (compose.poller.yml's bind mount).
KAFKA_BOOTSTRAP ?= 192.168.2.40:9092

replay-asap:
	docker compose -f compose.poller.yml run --rm --no-deps poller \
		python3 replay.py --dir /data/raw/vehicle_positions --kafka $(KAFKA_BOOTSTRAP) --speed asap

replay-realtime:
	docker compose -f compose.poller.yml run --rm --no-deps poller \
		python3 replay.py --dir /data/raw/vehicle_positions --kafka $(KAFKA_BOOTSTRAP) --speed realtime

gtfs-load:
	docker compose -f compose.infra.yml run --rm --build gtfs-loader

weather-load:
	docker compose -f compose.infra.yml run --rm --build weather-loader

restrictions-load:
	docker compose -f compose.infra.yml run --rm --build road-restrictions-loader

checkpoint:
	docker exec -it ttc-spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0 \
		/opt/spark-apps/00_checkpoint.py

dbt-build:
	docker compose -f compose.infra.yml run --rm --build dbt dbt build

dbt-test:
	docker compose -f compose.infra.yml run --rm --build dbt dbt test

dbt-docs:
	docker compose -f compose.infra.yml run --rm --build -p 8087:8080 dbt \
		sh -c "dbt docs generate && dbt docs serve --host 0.0.0.0 --port 8080"

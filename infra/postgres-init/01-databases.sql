-- Runs automatically ONLY on first container start (Postgres convention:
-- anything in /docker-entrypoint-initdb.d runs once, when the data
-- directory is empty). If you need to re-run this, you'd have to
-- `docker compose down -v` to wipe the volume first.

CREATE DATABASE metastore;
CREATE DATABASE airflow;

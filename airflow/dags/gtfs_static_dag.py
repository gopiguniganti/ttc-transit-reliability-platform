"""Runs the Phase 3 loader on a schedule instead of by hand via `make
gtfs-load`. Weekly, since TTC doesn't republish the static schedule daily."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gtfs_static_load",
    description="Load TTC's GTFS static feed (routes/stops/trips) into Postgres",
    schedule="0 3 * * 0",  # 3am every Sunday
    start_date=datetime(2026, 8, 1),
    catchup=False,
    default_args=default_args,
    tags=["ttc-platform"],
) as dag:
    BashOperator(
        task_id="load_gtfs_static",
        bash_command="python3 /opt/airflow/jobs/gtfs_static/load_dimensions.py",
    )

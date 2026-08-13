"""Runs the Phase 6 weather loader hourly -- matches how often Environment
Canada's city-page station actually updates, so more often would just
re-check a value that hasn't changed (and the UNIQUE constraint in
fact_weather would silently no-op those runs anyway)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_load",
    description="Load current Toronto conditions from Environment Canada",
    schedule="@hourly",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    default_args=default_args,
    tags=["ttc-platform"],
) as dag:
    BashOperator(
        task_id="load_weather",
        bash_command="python3 /opt/airflow/jobs/context_data/load_weather.py",
    )

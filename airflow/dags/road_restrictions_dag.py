"""Runs the Phase 6 road restrictions loader (+ its spatial join against
dim_stop) daily -- construction/closures don't change minute to minute."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="road_restrictions_load",
    description="Load Toronto road restrictions and spatial-join against stops",
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    default_args=default_args,
    tags=["ttc-platform"],
) as dag:
    BashOperator(
        task_id="load_road_restrictions",
        bash_command="python3 /opt/airflow/jobs/context_data/load_road_restrictions.py",
    )

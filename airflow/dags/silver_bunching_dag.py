"""Runs the Phase 5 silver/bunching batch job every 15 minutes.

Unlike the other DAGs, this doesn't run Spark locally in the Airflow
container -- spark-submit here just connects to the existing standalone
cluster at spark-master:7077 over the network and lets it do the actual
work, the same way `docker exec ttc-spark-master spark-submit ...` did
by hand while building this. --conf spark.cores.max=4 matches the cap
already in place on the streaming job, so this doesn't starve it."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SPARK_SUBMIT = (
    "spark-submit --master spark://spark-master:7077 --conf spark.cores.max=4 "
    "--packages org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.4 "
    "/opt/airflow/jobs/spark_jobs/02_silver_bunching.py"
)

with DAG(
    dag_id="silver_bunching_load",
    description="Silver layer + bunching heuristic, bronze Delta -> Postgres",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 8, 1),
    catchup=False,
    default_args=default_args,
    tags=["ttc-platform"],
) as dag:
    BashOperator(
        task_id="run_silver_bunching",
        bash_command=SPARK_SUBMIT,
    )

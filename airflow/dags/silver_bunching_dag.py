"""The "run the whole pipeline" DAG -- trigger this one manually any time
you want bronze -> Grafana refreshed right now (e.g. after `make on` and
a poller replay), and leave it unpaused to also get it refreshed
automatically every 15 minutes without touching anything.

Two tasks, chained:
  1. run_silver_bunching -- the Phase 5 batch job. Unlike the other DAGs,
     this doesn't run Spark locally in the Airflow container --
     spark-submit here just connects to the existing standalone cluster
     at spark-master:7077 over the network and lets it do the actual
     work, the same way `docker exec ttc-spark-master spark-submit ...`
     did by hand while building this. --conf spark.cores.max=4 matches
     the cap already in place on the streaming job, so this doesn't
     starve it.
  2. run_dbt_build -- rebuilds the staging views + gold marts Grafana
     reads from, same as `make dbt-build` but wired into the schedule
     instead of a separate manual step.

Both steps are safe to (re)run any number of times a day: the Spark job
overwrites/truncates its outputs from a full re-scan of bronze each run
(see 02_silver_bunching.py's docstring), and dbt's marts are full-table
rebuilds too -- neither appends, so nothing double-counts.

This does NOT include replaying the poller's raw archive into Kafka --
that's `make replay-asap`, and it has to run on the collector host
(ProDesk), not here, because that's where the raw files and
compose.poller.yml live. Skippable most of the time: the poller's Kafka
producer reconnects on its own once this host is back up, so ordinary
new polls flow through automatically -- replay is only for backfilling
whatever the poller collected *while this host was off*, and that data
stays safely on ProDesk's disk until you do replay it, whenever that is.
"""
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
    description="Run the pipeline: silver + bunching heuristic, then dbt build -- bronze Delta all the way to Grafana",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 8, 1),
    catchup=False,
    default_args=default_args,
    tags=["ttc-platform"],
) as dag:
    run_silver_bunching = BashOperator(
        task_id="run_silver_bunching",
        bash_command=SPARK_SUBMIT,
    )
    run_dbt_build = BashOperator(
        task_id="run_dbt_build",
        bash_command="cd /opt/airflow/jobs/dbt && dbt build",
    )
    run_silver_bunching >> run_dbt_build

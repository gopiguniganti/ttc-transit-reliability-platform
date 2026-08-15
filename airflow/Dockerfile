# Base image already has Airflow + a matching Python; we're only adding
# what our own DAGs need: the same DB/HTTP libs the loader scripts use,
# a JVM (spark-submit's driver is a JVM process, same reason Jupyter's
# Dockerfile needs one), and pyspark itself -- pyspark's spark-submit can
# talk to a REMOTE cluster over the network, so this container doesn't
# need to run Spark locally, just reach spark-master:7077.
FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# spark-submit's Ivy dependency resolver expects this directory to already
# exist -- it doesn't create it on first use, it just fails with a
# FileNotFoundException. Pre-creating it here means Docker seeds it into
# the airflow_ivy volume the first time that volume is mounted.
RUN mkdir -p /home/airflow/.ivy2/cache && chown -R airflow:root /home/airflow/.ivy2
# 02_silver_bunching.py's spark.eventLog.dir also isn't auto-created --
# unlike spark-master/workers, this container doesn't mount the shared
# spark_events volume (that volume is root-owned, same reason as .ivy2
# above), so event logs written here are just local and ephemeral, not
# visible in the History Server. Fine: this only affects viewing this
# specific job's DAG/stages there, not whether the job itself runs.
RUN mkdir -p /tmp/spark-events && chown airflow:root /tmp/spark-events
USER airflow

COPY airflow/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The loader scripts run as subprocesses (BashOperator), not imported --
# simplest way to reuse the exact same scripts Phase 3/6 already built and
# tested, no rewriting them as Airflow-flavored code. Build context is the
# repo root (see compose.infra.yml) so these paths reach outside airflow/.
COPY --chown=airflow:root gtfs_static/ /opt/airflow/jobs/gtfs_static/
COPY --chown=airflow:root context_data/ /opt/airflow/jobs/context_data/
COPY --chown=airflow:root notebooks/02_silver_bunching.py /opt/airflow/jobs/spark_jobs/02_silver_bunching.py

# dbt build, so the "run the pipeline" DAG can refresh the gold layer
# itself instead of that being a separate `make dbt-build` on the host.
# profiles.yml's postgres host already defaults to "postgres" (the
# service name), which resolves the same way from this container.
COPY --chown=airflow:root dbt/ /opt/airflow/jobs/dbt/
ENV DBT_PROFILES_DIR=/opt/airflow/jobs/dbt

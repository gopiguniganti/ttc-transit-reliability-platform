"""Silver layer + a bunching heuristic, run as a one-off batch job (not
streaming -- this re-scans the whole bronze table each run, which is fine
at this data volume and simpler than an incremental job).

Silver: bronze vehicle positions filtered to rows with a real GPS fix and
a known route, joined against dim_route (Postgres, via JDBC) for a
human-readable route name. Written to s3a://silver/vehicle_positions.

Bunching: a proxy signal, not a precise one -- see the caveat below --
flagging when two vehicles on the same route+direction, in the same poll
snapshot, are within one stop of each other in current_stop_sequence.
Written to Postgres's bunching_events table (also via JDBC, the reverse
direction of the same connector).

Run:
  docker exec -it ttc-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.4 \
    /opt/spark-apps/02_silver_bunching.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag, current_timestamp

JDBC_URL = "jdbc:postgresql://postgres:5432/serving"
JDBC_PROPS = {"user": "ttc", "password": "ttc_dev_password", "driver": "org.postgresql.Driver"}

spark = (
    SparkSession.builder
    .appName("silver-bunching")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "file:/tmp/spark-events")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.format("delta").load("s3a://bronze/vehicle_positions_stream")

# Real bug found while building the Grafana dashboard: a Kafka crash-loop
# during the backlog replay (spark-streaming restarted 5 times -- see
# docs/architecture.md) left ~2.9M (vehicle_id, feed_timestamp) groups with
# duplicate rows in bronze -- a batch read but not cleanly checkpointed
# before Kafka died got reprocessed on restart. Undetected, this fed
# identical back-to-back rows into the bunching window function below,
# which made lag() match a vehicle against its own duplicate --
# ~44% of "bunching events" turned out to be a vehicle bunched with
# itself. Deduplicating here, before anything downstream sees these rows,
# fixes it at the source rather than filtering the symptom out later.
bronze = bronze.dropDuplicates(["vehicle_id", "feed_timestamp"])

clean = bronze.filter(
    col("latitude").isNotNull()
    & col("longitude").isNotNull()
    & col("route_id").isNotNull()
    & col("current_stop_sequence").isNotNull()
)

dim_route = (
    spark.read.jdbc(JDBC_URL, "dim_route", properties=JDBC_PROPS)
    .filter(col("is_current"))
    .select("route_id", "route_short_name", "route_long_name")
)

silver = clean.join(dim_route, on="route_id", how="left")

(
    silver.write.format("delta").mode("overwrite")
    .save("s3a://silver/vehicle_positions")
)
print(f"SILVER_ROWS: {silver.count()}")

# Bunching heuristic: within the same route + poll snapshot (feed_timestamp),
# order vehicles by their position along the route (current_stop_sequence)
# and flag any pair one stop or less apart.
#
# NOT split by direction, even though that would be more precise -- checked
# two ways to get it and both failed on real data:
#   1. GTFS-RT's own direction_id field: 100% null across all 291k+ rows
#      collected so far. TTC's live feed simply never populates it.
#   2. Joining trip_id against dim_trip (which DOES have direction_id, from
#      the static schedule): only matched 151 of ~180,000 non-null trip_ids.
#      The live feed's trip_ids don't correspond to whatever static GTFS
#      version happens to be loaded -- TTC republishes the static schedule
#      periodically and regenerates trip_id each time (the same fact noted
#      in Phase 3 for why dim_trip isn't SCD2), so the two feeds' trip_ids
#      are only aligned right when they're from the very same publish.
# Net effect: this can flag two vehicles going OPPOSITE directions on the
# same route as "bunched" if their stop_sequence values happen to be close.
# Reconciling real-time and static trip_id versions is future work, not
# solved here -- documented instead of silently producing a wrong number.
#
# Separately, current_stop_sequence itself is only comparable within the
# same trip pattern -- a short-turn branch can reuse similar sequence
# numbers for physically distant stops. A fully precise version would use
# shape-distance-traveled from shapes.txt (not loaded in Phase 3 -- ~17MB
# of geometry not needed until now). Treat this as a first signal, not a
# claim of exact GPS-distance bunching.
window = Window.partitionBy("route_id", "feed_timestamp").orderBy("current_stop_sequence")

bunching = (
    silver
    .withColumn("prev_vehicle_id", lag("vehicle_id").over(window))
    .withColumn("prev_stop_sequence", lag("current_stop_sequence").over(window))
    .filter(col("prev_vehicle_id").isNotNull())
    # A vehicle can't bunch with itself -- a defensive check, not the fix
    # (the dropDuplicates above is the fix). Kept as a second line of
    # defense: if any other source of duplicate/near-duplicate rows ever
    # slips past that dedup, this stops it from silently producing a
    # nonsense "self-bunching" event again.
    .filter(col("prev_vehicle_id") != col("vehicle_id"))
    .withColumn("stop_sequence_gap", col("current_stop_sequence") - col("prev_stop_sequence"))
    .filter(col("stop_sequence_gap") <= 1)
    .select(
        "route_id", "route_short_name", "feed_timestamp",
        col("prev_vehicle_id").alias("vehicle_id_a"),
        col("vehicle_id").alias("vehicle_id_b"),
        "stop_sequence_gap",
    )
    .withColumn("detected_at", current_timestamp())
)

bunching_count = bunching.count()
print(f"BUNCHING_EVENTS: {bunching_count}")

(
    bunching.write.jdbc(
        JDBC_URL, "bunching_events", mode="overwrite",
        # mode="overwrite" alone does DROP TABLE + CREATE -- which Postgres
        # refuses once anything depends on the table (hit this directly:
        # dbt's staging.stg_bunching_events view blocked it with "other
        # objects depend on it"). truncate=true makes Spark issue TRUNCATE
        # instead, which dbt's downstream view/table dependencies survive.
        properties={**JDBC_PROPS, "batchsize": "5000", "rewriteBatchedStatements": "true", "truncate": "true"},
    )
)

spark.stop()

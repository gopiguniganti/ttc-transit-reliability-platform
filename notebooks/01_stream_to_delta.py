"""Reads ttc.vehicle_positions from Kafka and appends it to a Delta table
on MinIO, in small micro-batches, resuming from a checkpoint on restart.

Run:
  docker exec -it ttc-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    /opt/spark-apps/01_stream_to_delta.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, FloatType,
)

KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC = "ttc.vehicle_positions"
DELTA_PATH = "s3a://bronze/vehicle_positions_stream"
CHECKPOINT_PATH = "s3a://bronze/_checkpoints/vehicle_positions_stream"

# Same shape as poller.py's SCHEMA -- the Kafka message value is the exact
# JSON that poller.py's publish_rows() sent.
ROW_SCHEMA = StructType([
    StructField("feed_timestamp", LongType()),
    StructField("ingest_timestamp", LongType()),
    StructField("entity_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("trip_id", StringType()),
    StructField("route_id", StringType()),
    StructField("direction_id", IntegerType()),
    StructField("latitude", FloatType()),
    StructField("longitude", FloatType()),
    StructField("bearing", FloatType()),
    StructField("speed", FloatType()),
    StructField("current_stop_sequence", IntegerType()),
    StructField("stop_id", StringType()),
    StructField("current_status", StringType()),
    StructField("vehicle_timestamp", LongType()),
    StructField("occupancy_status", StringType()),
])

spark = (
    SparkSession.builder
    .appName("stream-vehicle-positions")
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

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    # Only honored on the very first run -- once CHECKPOINT_PATH exists,
    # Spark resumes from the offsets recorded there instead.
    .option("startingOffsets", "earliest")
    .load()
)

rows = raw.select(
    from_json(col("value").cast("string"), ROW_SCHEMA).alias("row")
).select("row.*")

query = (
    rows.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start(DELTA_PATH)
)

query.awaitTermination()

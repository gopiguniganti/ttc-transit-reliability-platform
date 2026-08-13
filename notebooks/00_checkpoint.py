"""Connectivity check: Spark cluster -> MinIO over S3A -> Delta Lake
read/write. Every later job builds on this working.

Run:
  docker exec -it ttc-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0 \
    /opt/spark-apps/00_checkpoint.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("checkpoint-00")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")   # required for MinIO
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "file:/tmp/spark-events")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("STEP 1: DataFrame + cluster sanity check")
print("=" * 60)
df = spark.range(0, 1000).withColumn("squared", col("id") * col("id"))
df.show(5)
print(f"Row count: {df.count()}")

print("=" * 60)
print("STEP 2: write/read plain Parquet on MinIO")
print("=" * 60)
df.write.mode("overwrite").parquet("s3a://bronze/checkpoint/plain_parquet")
print("Write OK")

read_back = spark.read.parquet("s3a://bronze/checkpoint/plain_parquet")
print(f"Read back {read_back.count()} rows from MinIO")

print("=" * 60)
print("STEP 3: write/read Delta table on MinIO")
print("=" * 60)
df.write.format("delta").mode("overwrite").save("s3a://bronze/checkpoint/delta_table")
print("Delta write OK")

delta_read = spark.read.format("delta").load("s3a://bronze/checkpoint/delta_table")
print(f"Read back {delta_read.count()} rows from Delta table")

print("=" * 60)
print("STEP 4: Delta time travel")
print("=" * 60)
# Overwrite with fewer rows, then read version 0 back -- overwriting
# marks the old version superseded, it doesn't delete it.
df2 = spark.range(0, 500)
df2.write.format("delta").mode("overwrite").save("s3a://bronze/checkpoint/delta_table")

current = spark.read.format("delta").load("s3a://bronze/checkpoint/delta_table")
print(f"Current version row count (should be 500): {current.count()}")

old_version = (
    spark.read.format("delta")
    .option("versionAsOf", 0)
    .load("s3a://bronze/checkpoint/delta_table")
)
print(f"Version 0 row count via time travel (should be 1000): {old_version.count()}")

print("=" * 60)
print("ALL CHECKS PASSED" if old_version.count() == 1000 else "SOMETHING IS WRONG -- time travel returned the wrong count")
print("=" * 60)

spark.stop()

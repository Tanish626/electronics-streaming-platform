"""Inspect the bronze layer: row counts per topic and a duplicate check."""
import os

from pyspark.sql import functions as F

from common import build_spark

BUCKET = os.getenv("LAKE_BUCKET", "lakehouse")
OUT = f"s3a://{BUCKET}/bronze/events"

spark = build_spark("bronze-check")
df = spark.read.parquet(OUT)

total = df.count()
distinct_keys = df.select("topic", "kafka_partition", "kafka_offset").distinct().count()

print(f"[check] total rows: {total}")
print(f"[check] duplicate (topic, partition, offset) rows: {total - distinct_keys}")
(
    df.groupBy("topic")
    .agg(
        F.count("*").alias("rows"),
        F.min("kafka_offset").alias("min_offset"),
        F.max("kafka_offset").alias("max_offset"),
        F.max("ingested_at").alias("last_ingested_at"),
    )
    .orderBy("topic")
    .show(truncate=False)
)
df.select("topic", "ingest_date", "kafka_partition", "kafka_offset", "raw_json").show(2, truncate=70)
spark.stop()
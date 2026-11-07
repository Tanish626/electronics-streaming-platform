"""Bronze layer: Kafka (all topics) -> raw Parquet on S3, as a streaming job.

Stores events untouched (raw JSON string + Kafka metadata). All parsing,
deduplication and validation happens later, in the silver layer.

Env vars:
  KAFKA_BOOTSTRAP_SERVERS, LAKE_BUCKET        connection / location
  BRONZE_TOPICS                               comma-separated topics
  BRONZE_TRIGGER                              "processing_time" (default, runs forever)
                                              or "available_now" (process backlog, then stop)
  BRONZE_INTERVAL                             micro-batch interval, e.g. "30 seconds"
  BRONZE_MAX_OFFSETS_PER_TRIGGER              cap on messages per micro-batch
"""
import os

from pyspark.sql import functions as F

from common import build_spark

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BUCKET = os.getenv("LAKE_BUCKET", "lakehouse")
TOPICS = os.getenv("BRONZE_TOPICS", "orders,page_views,cart_events,price_changes,inventory_updates")
TRIGGER = os.getenv("BRONZE_TRIGGER", "processing_time")
INTERVAL = os.getenv("BRONZE_INTERVAL", "30 seconds")
MAX_OFFSETS = os.getenv("BRONZE_MAX_OFFSETS_PER_TRIGGER", "100000")

OUT = f"s3a://{BUCKET}/bronze/events"
CHECKPOINT = f"s3a://{BUCKET}/_checkpoints/bronze_events"

spark = build_spark("bronze-stream")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPICS)
    .option("startingOffsets", "earliest")      # only used on the very first run
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .option("failOnDataLoss", "false")          # local dev: topic retention is 48h
    .load()
)

bronze = (
    raw.select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("event_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.current_timestamp().alias("ingested_at"),
    )
    .withColumn("ingest_date", F.to_date("ingested_at"))
)

writer = (
    bronze.writeStream.format("parquet")
    .outputMode("append")
    .option("path", OUT)
    .option("checkpointLocation", CHECKPOINT)
    .partitionBy("topic", "ingest_date")
)

if TRIGGER in ("available_now", "once"):
    writer = writer.trigger(availableNow=True)
else:
    writer = writer.trigger(processingTime=INTERVAL)

query = writer.start()
print(f"[bronze] streaming {TOPICS} -> {OUT} (trigger={TRIGGER})", flush=True)
query.awaitTermination()
print("[bronze] query finished", flush=True)
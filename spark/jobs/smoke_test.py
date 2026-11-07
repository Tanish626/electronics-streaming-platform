"""Smoke test: Kafka (batch read) -> Parquet on S3 -> read back.

Proves the Kafka connector, the S3A connector and the object store all work
together before we write the real streaming job.
"""
import os
import sys

from common import build_spark

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BUCKET = os.getenv("LAKE_BUCKET", "lakehouse")
TOPIC = "orders"
OUT = f"s3a://{BUCKET}/_smoke/{TOPIC}_raw"

spark = build_spark("smoke-test")

raw = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

df = raw.selectExpr(
    "CAST(key AS STRING) AS key",
    "CAST(value AS STRING) AS value",
    "topic", "partition", "offset", "timestamp",
)

kafka_count = df.count()
print(f"[smoke] messages read from Kafka topic '{TOPIC}': {kafka_count}")
if kafka_count == 0:
    print("[smoke] topic is empty. Run the simulator for a few seconds and retry.")
    sys.exit(1)

df.write.mode("overwrite").parquet(OUT)
print(f"[smoke] wrote Parquet to {OUT}")

back = spark.read.parquet(OUT)
back_count = back.count()
print(f"[smoke] rows read back from S3: {back_count}")
back.select("topic", "partition", "offset", "value").show(3, truncate=60)

print("[smoke] OK" if back_count == kafka_count else "[smoke] MISMATCH")
spark.stop()
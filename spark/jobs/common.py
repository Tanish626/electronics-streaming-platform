"""Shared Spark session setup for all jobs. Configuration comes from env vars."""
import os

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        # S3A -> S3-compatible object store (SeaweedFS)
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://seaweedfs:8333"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # small local data: far fewer shuffle partitions than the default 200
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
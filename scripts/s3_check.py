"""Smoke test for the S3-compatible object store (write, read, list, delete).

Reads settings from .env (see .env.example). Run from the repo root.
"""
import os
import sys

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

ENDPOINT = os.getenv("S3_ENDPOINT", f"http://localhost:{os.getenv('S3_API_PORT', '8333')}")
ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "lakeadmin")
SECRET_KEY = os.getenv("S3_SECRET_KEY", "lakesecret123")
BUCKET = os.getenv("LAKE_BUCKET", "lakehouse")
KEY = "_healthcheck/hello.txt"


def main() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),  # required for non-AWS S3
    )
    try:
        buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
        print(f"buckets: {buckets}")
        if BUCKET not in buckets:
            print(f"ERROR: bucket '{BUCKET}' not found")
            sys.exit(1)

        s3.put_object(Bucket=BUCKET, Key=KEY, Body=b"hello from the simulator repo")
        body = s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read().decode()
        print(f"read back: {body!r}")

        listed = s3.list_objects_v2(Bucket=BUCKET, Prefix="_healthcheck/")
        print(f"objects under prefix: {[o['Key'] for o in listed.get('Contents', [])]}")

        s3.delete_object(Bucket=BUCKET, Key=KEY)
        print("OK: S3 API works (list, put, get, delete)")
    except (ClientError, BotoCoreError) as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
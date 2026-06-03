"""Fargate entry point — reads INPUT_BUCKET, INPUT_KEY, and optional OUTPUT_KEY from env vars."""
import os
import boto3
from log_processor import process

INPUT_BUCKET = os.environ["INPUT_BUCKET"]
INPUT_KEY    = os.environ["INPUT_KEY"]
OUTPUT_KEY   = os.environ.get("OUTPUT_KEY")  # optional override for chunked processing

if __name__ == "__main__":
    s3 = boto3.client("s3")
    print(f"Processing s3://{INPUT_BUCKET}/{INPUT_KEY}")
    process(s3, INPUT_BUCKET, INPUT_KEY, output_key_override=OUTPUT_KEY)
    print("Done.")

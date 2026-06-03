import boto3
import os
import time
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

SOURCE_KEY_ID     = os.environ["SOURCE_AWS_ACCESS_KEY_ID"]
SOURCE_SECRET     = os.environ["SOURCE_AWS_SECRET_ACCESS_KEY"]
SOURCE_BUCKET     = os.environ.get("SOURCE_BUCKET", "raw-resolution-logs.datacite.org")
SOURCE_PREFIX     = os.environ.get("SOURCE_PREFIX", "202604/")

DEST_KEY_ID       = os.environ["DEST_AWS_ACCESS_KEY_ID"]
DEST_SECRET       = os.environ["DEST_AWS_SECRET_ACCESS_KEY"]
DEST_BUCKET       = os.environ.get("DEST_BUCKET", "datacite-logs")

CHUNK_SIZE        = 25 * 1024 * 1024   # 25 MB
MAX_RETRIES       = 5

BOTO_CONFIG = Config(
    retries={"max_attempts": MAX_RETRIES, "mode": "adaptive"},
    read_timeout=120,
    connect_timeout=30,
)

def get_chunk(src_s3, key, offset, end, attempt=0):
    try:
        resp = src_s3.get_object(
            Bucket=SOURCE_BUCKET, Key=key,
            Range=f"bytes={offset}-{end}"
        )
        return resp["Body"].read()
    except Exception as e:
        if attempt < MAX_RETRIES:
            wait = 2 ** attempt
            print(f"  read error, retrying in {wait}s ({e})")
            time.sleep(wait)
            return get_chunk(src_s3, key, offset, end, attempt + 1)
        raise

def copy_file(src_s3, dst_s3, key):
    print(f"Copying {key} ...")

    head = src_s3.head_object(Bucket=SOURCE_BUCKET, Key=key)
    total = head["ContentLength"]

    mpu = dst_s3.create_multipart_upload(Bucket=DEST_BUCKET, Key=key)
    upload_id = mpu["UploadId"]
    parts = []

    try:
        part_num = 1
        offset = 0
        while offset < total:
            end = min(offset + CHUNK_SIZE - 1, total - 1)
            data = get_chunk(src_s3, key, offset, end)
            part = dst_s3.upload_part(
                Bucket=DEST_BUCKET, Key=key,
                UploadId=upload_id, PartNumber=part_num, Body=data
            )
            parts.append({"PartNumber": part_num, "ETag": part["ETag"]})
            pct = (end + 1) / total * 100
            print(f"  part {part_num} ({pct:.1f}%)", flush=True)
            part_num += 1
            offset = end + 1

        dst_s3.complete_multipart_upload(
            Bucket=DEST_BUCKET, Key=key, UploadId=upload_id,
            MultipartUpload={"Parts": parts}
        )
        print(f"  done: {key}", flush=True)

    except Exception as e:
        dst_s3.abort_multipart_upload(Bucket=DEST_BUCKET, Key=key, UploadId=upload_id)
        raise e

def main():
    src_s3 = boto3.client(
        "s3",
        aws_access_key_id=SOURCE_KEY_ID,
        aws_secret_access_key=SOURCE_SECRET,
        config=BOTO_CONFIG,
    )
    dst_s3 = boto3.client(
        "s3",
        aws_access_key_id=DEST_KEY_ID,
        aws_secret_access_key=DEST_SECRET,
        region_name="us-east-2",
        config=BOTO_CONFIG,
    )

    paginator = src_s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=SOURCE_PREFIX):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    print(f"Found {len(keys)} files to copy\n", flush=True)
    for key in keys:
        copy_file(src_s3, dst_s3, key)

    print("\nAll done.")

if __name__ == "__main__":
    main()

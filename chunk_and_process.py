"""
Chunks raw .gz log files in S3 and launches parallel Fargate tasks.

Usage:
  python chunk_and_process.py [--chunks N] [--key <specific key>]

Streams each .gz from S3, round-robins lines into N gzip chunk files,
uploads chunks back to S3, then launches N Fargate tasks in parallel.
"""

import argparse
import boto3
import gzip
import io
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

SOURCE_KEY_ID    = os.environ["DEST_AWS_ACCESS_KEY_ID"]
SOURCE_SECRET    = os.environ["DEST_AWS_SECRET_ACCESS_KEY"]
SOURCE_BUCKET    = os.environ.get("DEST_BUCKET", "datacite-logs")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "datacite-logs-processed")

ALL_KEYS = [
    "202605/DataCite-access.log-202605-ap-southeast-1.gz",
    "202605/DataCite-access.log-202605-eu-west-1.gz",
    "202605/DataCite-access.log-202605-us-east-1.gz",
    "202605/DataCite-access.log-202605-us-west-2.gz",
]

CLUSTER      = os.environ.get("ECS_CLUSTER", "datacite-logs")
TASK_DEF     = os.environ.get("ECS_TASK_DEF", "datacite-log-processor")
SUBNET       = os.environ.get("ECS_SUBNET", "")
REGION       = os.environ.get("AWS_REGION", "us-east-2")
MIN_PART     = 5 * 1024 * 1024  # 5 MB S3 multipart minimum


class _MultipartGzipWriter:
    """Streams gzip-compressed data to S3 via multipart upload."""

    def __init__(self, s3, bucket, key):
        self._s3 = s3
        self._bucket = bucket
        self._key = key
        self._buf = bytearray()
        self._parts = []
        resp = s3.create_multipart_upload(Bucket=bucket, Key=key)
        self._upload_id = resp["UploadId"]
        self._gz_buf = io.BytesIO()
        self._gz = gzip.GzipFile(fileobj=self._gz_buf, mode="wb")

    def write_line(self, line: bytes) -> None:
        self._gz.write(line)
        if self._gz_buf.tell() >= MIN_PART:
            self._flush()

    def _flush(self) -> None:
        self._gz.close()  # finalize the gzip member (writes CRC32 + footer)
        size = self._gz_buf.tell()
        if size == 0:
            return
        self._gz_buf.seek(0)
        data = self._gz_buf.read(size)
        self._gz_buf = io.BytesIO()
        self._gz = gzip.GzipFile(fileobj=self._gz_buf, mode="wb", compresslevel=1)
        part_num = len(self._parts) + 1
        resp = self._s3.upload_part(
            Bucket=self._bucket, Key=self._key,
            UploadId=self._upload_id, PartNumber=part_num, Body=data,
        )
        self._parts.append({"PartNumber": part_num, "ETag": resp["ETag"]})

    def complete(self) -> None:
        self._gz.close()  # finalizes gzip stream, writes footer into self._gz_buf
        size = self._gz_buf.tell()
        if size > 0:
            self._gz_buf.seek(0)
            data = self._gz_buf.read(size)
            part_num = len(self._parts) + 1
            resp = self._s3.upload_part(
                Bucket=self._bucket, Key=self._key,
                UploadId=self._upload_id, PartNumber=part_num, Body=data,
            )
            self._parts.append({"PartNumber": part_num, "ETag": resp["ETag"]})
        if not self._parts:
            self._s3.abort_multipart_upload(
                Bucket=self._bucket, Key=self._key, UploadId=self._upload_id
            )
            return
        self._s3.complete_multipart_upload(
            Bucket=self._bucket, Key=self._key, UploadId=self._upload_id,
            MultipartUpload={"Parts": self._parts},
        )


def chunk_file(s3, key: str, n: int) -> list[str]:
    """Stream key from S3, split into n chunk files. Returns list of chunk keys."""
    stem = key.split("/")[-1].replace(".gz", "")
    prefix = key.rsplit("/", 1)[0]
    chunk_keys = [f"{prefix}/chunks/{stem}-chunk-{i:03d}.gz" for i in range(n)]

    print(f"  Splitting {key} into {n} chunks...")
    writers = [_MultipartGzipWriter(s3, SOURCE_BUCKET, ck) for ck in chunk_keys]

    body = s3.get_object(Bucket=SOURCE_BUCKET, Key=key)["Body"]
    line_count = 0
    with gzip.GzipFile(fileobj=body) as gz:
        for raw in gz:
            writers[line_count % n].write_line(raw)
            line_count += 1
            if line_count % 5_000_000 == 0:
                print(f"    {line_count:,} lines split...", flush=True)

    for w in writers:
        w.complete()

    print(f"  Split complete: {line_count:,} lines → {n} chunks")
    return chunk_keys


def launch_tasks(s3, ecs, chunk_keys: list[str], output_bucket: str) -> list[str]:
    """Launch one Fargate task per chunk. Returns list of task ARNs."""
    task_arns = []
    for i, chunk_key in enumerate(chunk_keys):
        stem = chunk_key.split("/")[-1].replace(".gz", "")
        original_stem = chunk_key.split("/chunks/")[1].split("-chunk-")[0]
        parts = original_stem.split("-")
        region_str = "-".join(parts[3:]).replace("-chunk", "")
        output_key = f"datacite-logs/year=2026/month=5/region={region_str}/{stem}.parquet"

        resp = ecs.run_task(
            cluster=CLUSTER,
            taskDefinition=TASK_DEF,
            launchType="FARGATE",
            networkConfiguration={"awsvpcConfiguration": {
                "subnets": [SUBNET],
                "assignPublicIp": "ENABLED",
            }},
            overrides={"containerOverrides": [{
                "name": "log-processor",
                "environment": [
                    {"name": "INPUT_BUCKET",  "value": SOURCE_BUCKET},
                    {"name": "INPUT_KEY",     "value": chunk_key},
                    {"name": "OUTPUT_KEY",    "value": output_key},
                    {"name": "OUTPUT_BUCKET", "value": output_bucket},
                ],
            }]},
        )
        if resp["failures"]:
            print(f"  WARN task {i} failed to launch: {resp['failures']}")
        else:
            arn = resp["tasks"][0]["taskArn"]
            task_arns.append(arn)
            print(f"  Launched chunk {i}: {arn.split('/')[-1]}")

    return task_arns


def process_key(key: str, n_chunks: int, output_bucket: str) -> tuple[str, list[str]]:
    """Chunk a single key and launch its Fargate tasks. Runs in a thread."""
    s3  = boto3.client("s3",  aws_access_key_id=SOURCE_KEY_ID, aws_secret_access_key=SOURCE_SECRET, region_name=REGION)
    ecs = boto3.client("ecs", aws_access_key_id=SOURCE_KEY_ID, aws_secret_access_key=SOURCE_SECRET, region_name=REGION)
    print(f"\n{'='*60}\nProcessing: {key}")
    chunk_keys = chunk_file(s3, key, n_chunks)
    task_arns = launch_tasks(s3, ecs, chunk_keys, output_bucket)
    print(f"  Launched {len(task_arns)} tasks for {key}")
    return key, task_arns


def wait_for_tasks(ecs, task_arns: list[str]) -> bool:
    """Poll until all tasks stop. Returns True if all succeeded."""
    pending = set(task_arns)
    while pending:
        time.sleep(30)
        resp = ecs.describe_tasks(cluster=CLUSTER, tasks=list(pending))
        for t in resp["tasks"]:
            if t["lastStatus"] == "STOPPED":
                arn = t["taskArn"]
                exit_code = t["containers"][0].get("exitCode", -1)
                status = "OK" if exit_code == 0 else f"FAILED (exit {exit_code})"
                print(f"  Task {arn.split('/')[-1]} → {status}")
                pending.discard(arn)
        if pending:
            print(f"  {len(pending)} tasks still running...")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunks", type=int, default=8, help="Number of chunks per file (default: 8, uses 64 vCPU across 4 regions)")
    parser.add_argument("--key", help="Process a single key instead of all files")
    parser.add_argument("--output-bucket", dest="output_bucket", default=PROCESSED_BUCKET,
                        help="S3 bucket for Parquet output (default: PROCESSED_BUCKET)")
    args = parser.parse_args()

    keys = [args.key] if args.key else ALL_KEYS
    all_task_arns: list[str] = []

    with ThreadPoolExecutor(max_workers=len(keys)) as pool:
        futures = {pool.submit(process_key, key, args.chunks, args.output_bucket): key for key in keys}
        for future in as_completed(futures):
            key, task_arns = future.result()
            all_task_arns.extend(task_arns)

    ecs = boto3.client("ecs", aws_access_key_id=SOURCE_KEY_ID, aws_secret_access_key=SOURCE_SECRET, region_name=REGION)
    print(f"\nWaiting for {len(all_task_arns)} total tasks...")
    wait_for_tasks(ecs, all_task_arns)
    print("\nAll files processed.")


if __name__ == "__main__":
    main()

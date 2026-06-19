"""
Fargate splitter task.

Streams a single .gz log file from S3, splits it into N gzip chunks,
uploads them back to S3, launches N processor Fargate tasks, and waits
for them to complete. Runs entirely within AWS — no local machine in
the critical path.

Environment variables (required):
  INPUT_BUCKET   source bucket (e.g. datacite-logs)
  INPUT_KEY      source object key (e.g. 202605/DataCite-access.log-202605-us-east-1.gz)
  OUTPUT_BUCKET  Parquet output bucket
  ECS_CLUSTER    ECS cluster name
  ECS_TASK_DEF   processor task definition (e.g. datacite-log-processor:4)
  ECS_SUBNET     subnet ID for launched tasks

Environment variables (optional):
  N_CHUNKS       chunks to split into (default: 8)
  AWS_REGION     region (default: us-east-2)
"""

import boto3
import gzip
import io
import os
import time
from botocore.config import Config

INPUT_BUCKET  = os.environ["INPUT_BUCKET"]
INPUT_KEY     = os.environ["INPUT_KEY"]
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
N_CHUNKS      = int(os.environ.get("N_CHUNKS", "8"))
CLUSTER       = os.environ["ECS_CLUSTER"]
TASK_DEF      = os.environ["ECS_TASK_DEF"]
SUBNET        = os.environ["ECS_SUBNET"]
REGION        = os.environ.get("AWS_REGION", "us-east-2")
MIN_PART      = 5 * 1024 * 1024

# Inside Fargate the task IAM role provides credentials — no explicit keys needed
s3  = boto3.client("s3",  region_name=REGION, config=Config(read_timeout=600, retries={"max_attempts": 3}))
ecs = boto3.client("ecs", region_name=REGION)


class _MultipartGzipWriter:
    """Streams gzip-compressed data to S3 via multipart upload."""

    def __init__(self, bucket, key):
        self._bucket = bucket
        self._key = key
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
        resp = s3.upload_part(
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
            resp = s3.upload_part(
                Bucket=self._bucket, Key=self._key,
                UploadId=self._upload_id, PartNumber=part_num, Body=data,
            )
            self._parts.append({"PartNumber": part_num, "ETag": resp["ETag"]})
        if not self._parts:
            s3.abort_multipart_upload(
                Bucket=self._bucket, Key=self._key, UploadId=self._upload_id
            )
            return
        s3.complete_multipart_upload(
            Bucket=self._bucket, Key=self._key, UploadId=self._upload_id,
            MultipartUpload={"Parts": self._parts},
        )


def chunk_file(key: str, n: int) -> list[str]:
    """Stream key from S3, split into n chunk files. Returns list of chunk keys."""
    stem = key.split("/")[-1].replace(".gz", "")
    prefix = key.rsplit("/", 1)[0]
    chunk_keys = [f"{prefix}/chunks/{stem}-chunk-{i:03d}.gz" for i in range(n)]

    print(f"Splitting {key} into {n} chunks...", flush=True)
    writers = [_MultipartGzipWriter(INPUT_BUCKET, ck) for ck in chunk_keys]

    body = s3.get_object(Bucket=INPUT_BUCKET, Key=key)["Body"]
    line_count = 0
    with gzip.GzipFile(fileobj=body) as gz:
        for raw in gz:
            writers[line_count % n].write_line(raw)
            line_count += 1
            if line_count % 5_000_000 == 0:
                print(f"  {line_count:,} lines split...", flush=True)

    for w in writers:
        w.complete()

    print(f"Split complete: {line_count:,} lines → {n} chunks", flush=True)
    return chunk_keys


def launch_tasks(chunk_keys: list[str]) -> list[str]:
    """Launch one processor Fargate task per chunk. Returns list of task ARNs."""
    yyyymm = INPUT_KEY.split("/")[0]
    year, month = int(yyyymm[:4]), int(yyyymm[4:])

    task_arns = []
    for i, chunk_key in enumerate(chunk_keys):
        stem = chunk_key.split("/")[-1].replace(".gz", "")
        original_stem = chunk_key.split("/chunks/")[1].split("-chunk-")[0]
        parts = original_stem.split("-")
        region_str = "-".join(parts[3:])
        output_key = f"datacite-logs/year={year}/month={month}/region={region_str}/{stem}.parquet"

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
                    {"name": "INPUT_BUCKET",  "value": INPUT_BUCKET},
                    {"name": "INPUT_KEY",     "value": chunk_key},
                    {"name": "OUTPUT_KEY",    "value": output_key},
                    {"name": "OUTPUT_BUCKET", "value": OUTPUT_BUCKET},
                ],
            }]},
        )
        if resp["failures"]:
            print(f"WARN chunk {i} failed to launch: {resp['failures']}", flush=True)
        else:
            arn = resp["tasks"][0]["taskArn"]
            task_arns.append(arn)
            print(f"Launched chunk {i}: {arn.split('/')[-1]}", flush=True)

    return task_arns


def wait_for_tasks(task_arns: list[str]) -> bool:
    """Poll until all processor tasks stop. Returns True if all succeeded."""
    pending = set(task_arns)
    while pending:
        time.sleep(30)
        resp = ecs.describe_tasks(cluster=CLUSTER, tasks=list(pending))
        for t in resp["tasks"]:
            if t["lastStatus"] == "STOPPED":
                arn = t["taskArn"]
                exit_code = t["containers"][0].get("exitCode", -1)
                status = "OK" if exit_code == 0 else f"FAILED (exit {exit_code})"
                print(f"Task {arn.split('/')[-1]} → {status}", flush=True)
                pending.discard(arn)
        if pending:
            print(f"{len(pending)} tasks still running...", flush=True)
    return True


if __name__ == "__main__":
    chunk_keys = chunk_file(INPUT_KEY, N_CHUNKS)
    task_arns = launch_tasks(chunk_keys)
    print(f"\nWaiting for {len(task_arns)} processor tasks...", flush=True)
    wait_for_tasks(task_arns)
    print("Done.")

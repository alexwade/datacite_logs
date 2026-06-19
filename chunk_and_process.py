"""
Launches one Fargate splitter task per log file.

Each splitter runs entirely within AWS: streams the .gz from S3, splits it
into N chunks, uploads them, launches N processor tasks, and waits for them.
The local machine is no longer in the critical path for data transfer.

Usage:
  python chunk_and_process.py [--chunks N] [--key <specific key>] [--output-bucket BUCKET]
"""

import argparse
import boto3
import os
import time
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

CLUSTER       = os.environ.get("ECS_CLUSTER", "datacite-logs")
TASK_DEF      = os.environ.get("ECS_TASK_DEF", "datacite-log-processor")
SPLITTER_DEF  = os.environ.get("ECS_SPLITTER_DEF", "datacite-log-splitter")
SUBNET        = os.environ.get("ECS_SUBNET", "")
REGION        = os.environ.get("AWS_REGION", "us-east-2")


def launch_splitters(ecs, keys: list[str], n_chunks: int, output_bucket: str) -> list[str]:
    """Launch one splitter Fargate task per key. Returns list of task ARNs."""
    task_arns = []
    for key in keys:
        resp = ecs.run_task(
            cluster=CLUSTER,
            taskDefinition=SPLITTER_DEF,
            launchType="FARGATE",
            networkConfiguration={"awsvpcConfiguration": {
                "subnets": [SUBNET],
                "assignPublicIp": "ENABLED",
            }},
            overrides={"containerOverrides": [{
                "name": "log-splitter",
                "environment": [
                    {"name": "INPUT_BUCKET",  "value": SOURCE_BUCKET},
                    {"name": "INPUT_KEY",     "value": key},
                    {"name": "OUTPUT_BUCKET", "value": output_bucket},
                    {"name": "N_CHUNKS",      "value": str(n_chunks)},
                    {"name": "ECS_CLUSTER",   "value": CLUSTER},
                    {"name": "ECS_TASK_DEF",  "value": TASK_DEF},
                    {"name": "ECS_SUBNET",    "value": SUBNET},
                ],
            }]},
        )
        if resp["failures"]:
            print(f"WARN failed to launch splitter for {key}: {resp['failures']}")
        else:
            arn = resp["tasks"][0]["taskArn"]
            task_arns.append(arn)
            print(f"Launched splitter for {key}: {arn.split('/')[-1]}")
    return task_arns


def wait_for_splitters(ecs, task_arns: list[str]) -> None:
    """Poll until all splitter tasks stop (each splitter waits for its own processors)."""
    pending = set(task_arns)
    while pending:
        time.sleep(30)
        resp = ecs.describe_tasks(cluster=CLUSTER, tasks=list(pending))
        for t in resp["tasks"]:
            if t["lastStatus"] == "STOPPED":
                arn = t["taskArn"]
                exit_code = t["containers"][0].get("exitCode", -1)
                status = "OK" if exit_code == 0 else f"FAILED (exit {exit_code})"
                print(f"Splitter {arn.split('/')[-1]} → {status}")
                pending.discard(arn)
        if pending:
            print(f"{len(pending)} splitter(s) still running...")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunks", type=int, default=8,
                        help="Chunks per file (default: 8)")
    parser.add_argument("--key", help="Process a single key instead of all files")
    parser.add_argument("--output-bucket", dest="output_bucket", default=PROCESSED_BUCKET,
                        help="S3 bucket for Parquet output (default: PROCESSED_BUCKET)")
    args = parser.parse_args()

    keys = [args.key] if args.key else ALL_KEYS
    ecs = boto3.client("ecs", aws_access_key_id=SOURCE_KEY_ID,
                       aws_secret_access_key=SOURCE_SECRET, region_name=REGION)

    print(f"Launching {len(keys)} splitter task(s) → {args.chunks} chunks each...")
    task_arns = launch_splitters(ecs, keys, args.chunks, args.output_bucket)
    print(f"\nWaiting for {len(task_arns)} splitter(s) to complete...")
    wait_for_splitters(ecs, task_arns)
    print("\nAll files processed.")


if __name__ == "__main__":
    main()

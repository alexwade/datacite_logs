#!/usr/bin/env python3
"""Remove redundant chunk-parquet pollution from a month's processed partitions.

WHEN YOU NEED THIS: the normal monthly flow is auto-triggered (S3 event -> Fargate)
and produces exactly ONE clean whole-file parquet per region:
    region=<region>/DataCite-access-YYYYMM-<region>.parquet
If someone ALSO runs chunk_and_process.py for a month that was already
auto-processed, it creates duplicate chunk parquets in the correct partitions
plus malformed  region=<region>-chunk-NNN/  partitions. This script deletes only
that pollution (any key containing 'chunk'), keeping the 4 clean whole-file
parquets, and also clears the intermediate .gz chunks in DEST/<month>/chunks/.

Usage:
    python scripts/cleanup_chunk_pollution.py --year 2026 --month 7            # dry run
    python scripts/cleanup_chunk_pollution.py --year 2026 --month 7 --execute  # delete

Refuses to delete unless the KEEP set is EXACTLY the 4 expected whole-file parquets.
"""
import argparse, os, sys, boto3
from dotenv import load_dotenv
load_dotenv()

REGIONS = ["ap-southeast-1", "eu-west-1", "us-east-1", "us-west-2"]

def list_keys(s3, bucket, prefix):
    keys, tok = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if tok:
            kw["ContinuationToken"] = tok
        resp = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in resp.get("Contents", [])]
        if resp.get("IsTruncated"):
            tok = resp["NextContinuationToken"]
        else:
            return keys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--month", type=int, required=True)
    ap.add_argument("--execute", action="store_true", help="actually delete (default: dry run)")
    args = ap.parse_args()

    y, m = args.year, args.month
    ym = f"{y}{m:02d}"
    processed = os.environ.get("PROCESSED_BUCKET", "datacite-logs-processed")
    dest = os.environ.get("DEST_BUCKET", "datacite-logs")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["DEST_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["DEST_AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-2"),
    )
    expected_keep = {
        f"datacite-logs/year={y}/month={m}/region={r}/DataCite-access-{ym}-{r}.parquet"
        for r in REGIONS
    }

    proc = list_keys(s3, processed, f"datacite-logs/year={y}/month={m}/")
    keep = sorted(k for k in proc if "chunk" not in k)
    delete_proc = sorted(k for k in proc if "chunk" in k)
    gz = sorted(list_keys(s3, dest, f"{ym}/chunks/"))

    print(f"{ym}: processed objects={len(proc)}  keep={len(keep)}  delete(pollution)={len(delete_proc)}  gz-chunks={len(gz)}")
    print("KEEP:")
    for k in keep:
        print("   ", k)

    if set(keep) != expected_keep:
        print("\n!! SAFETY STOP: KEEP != the 4 expected whole-file parquets. Nothing deleted.")
        print("   missing:", sorted(expected_keep - set(keep)))
        print("   unexpected:", sorted(set(keep) - expected_keep))
        sys.exit(1)
    print("SAFETY CHECK PASSED.")

    if not args.execute:
        print("DRY RUN — nothing deleted. Add --execute to delete the pollution.")
        return

    def batch_delete(bucket, keys):
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})
        return len(keys)

    nd = batch_delete(processed, delete_proc)
    ng = batch_delete(dest, gz)
    print(f"DELETED {nd} pollution parquets + {ng} .gz chunks.")
    remain = sorted(list_keys(s3, processed, f"datacite-logs/year={y}/month={m}/"))
    print(f"remaining objects: {len(remain)}")
    for k in remain:
        print("   ", k)

if __name__ == "__main__":
    main()

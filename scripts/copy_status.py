#!/usr/bin/env python3
"""Report overall % progress of the July-logs copy (copy_logs.py -> /tmp/copy_july.log)."""
import os, re, boto3
from dotenv import load_dotenv
load_dotenv()

# Source file sizes (bytes) for the four July 2026 region logs.
SIZES = {
    "ap-southeast-1": 1559086732,
    "eu-west-1": 2634665581,
    "us-east-1": 1382994332,
    "us-west-2": 1111513141,
}
TOTAL = sum(SIZES.values())
LOG = "/tmp/copy_july.log"

log = open(LOG).read() if os.path.exists(LOG) else ""

s3 = boto3.client("s3",
    aws_access_key_id=os.environ["DEST_AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["DEST_AWS_SECRET_ACCESS_KEY"],
    region_name=os.environ.get("AWS_REGION", "us-east-2"))
resp = s3.list_objects_v2(Bucket=os.environ.get("DEST_BUCKET", "datacite-logs"), Prefix="202607/")

done, done_bytes = set(), 0
for o in resp.get("Contents", []):
    for r, sz in SIZES.items():
        if o["Key"].endswith(f"-{r}.gz") and o["Size"] == sz:
            done.add(r); done_bytes += sz

cur = None
for m in re.finditer(r"DataCite-access\.log-202607-([a-z0-9-]+)\.gz", log):
    cur = m.group(1)
pcts = re.findall(r"part \d+ \((\d+\.?\d*)%\)", log)
cur_pct = float(pcts[-1]) if pcts else 0.0
cur_bytes = SIZES.get(cur, 0) * cur_pct / 100 if (cur and cur not in done) else 0

overall = 100 * (done_bytes + cur_bytes) / TOTAL
alldone = "All done" in log
# Only flag FATAL errors. copy_logs.py retries transient read errors ("read
# error, retrying …") and recovers, so don't treat those as failures.
err = ("Traceback" in log) or ("giving up" in log) or ("aborted after" in log)

print(f"completed: {len(done)}/4 files {sorted(done)}")
print(f"in-flight: {cur} @ {cur_pct:.1f}%")
print(f"OVERALL:   {overall:.1f}%  of {TOTAL/1e9:.2f} GB"
      + ("   [ALL DONE]" if alldone else "")
      + ("   [ERROR in log]" if err else ""))

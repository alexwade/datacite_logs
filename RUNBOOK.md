# Monthly Runbook — DataCite Resolution Logs

This is the step-by-step operational guide for ingesting one month of DataCite
DOI resolution logs into the Athena warehouse. Read [DESIGN.md](DESIGN.md) first
for the architecture; this document is the "what do I actually run each month."

> **⚠️ The single most important thing to know**
>
> **Copying a month's files into `s3://datacite-logs/YYYYMM/` AUTO-TRIGGERS
> processing.** An S3 event fires a Lambda/Fargate job that turns each copied
> `.gz` into exactly one Parquet file, in the right partition, on its own.
>
> So the normal monthly flow is just **copy → wait → register in Athena**. You
> should not need to launch any processing by hand; if a month's auto-trigger
> fails, see [Manual reprocess](#manual-reprocess-rare).

---

## 0. Prerequisites

- Python 3 with deps: `pip install boto3 python-dotenv pyathena pandas` (or use a venv).
- `cp .env.example .env` and fill in **both** credential sets:
  - `SOURCE_AWS_*` — DataCite's account (read-only on the raw bucket).
  - `DEST_AWS_*` — our account (read/write on staging + processed buckets, Athena).
- `.env` is gitignored — **never commit real credentials.**

## 1. Check whether the month's raw logs have landed

DataCite publishes each month's logs early in the *following* month (e.g. June
2026 appeared 2026-07-14; July 2026 appeared 2026-08-10). Four region files per
month: `ap-southeast-1`, `eu-west-1`, `us-east-1`, `us-west-2`.

```bash
set -a; . ./.env; set +a
AWS_ACCESS_KEY_ID="$SOURCE_AWS_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$SOURCE_AWS_SECRET_ACCESS_KEY" \
  aws s3 ls "s3://$SOURCE_BUCKET/202607/" --region "${AWS_REGION:-us-east-1}"
# expect 4 files: DataCite-access.log-202607-<region>.gz
```

## 2. Copy the month into the staging bucket

Set the month, then run the copy. It's a **cross-account** copy that streams
through the local machine (source and dest are different AWS accounts), so it is
bandwidth-bound and can take **several hours** for ~7 GB. It is **idempotent**
(skips files already present at matching size) and **retries** transient read
errors — occasional `Connection reset` / `Read timeout` lines in the log are
normal; it recovers.

```bash
# point the pipeline at the month
sed -i '' 's#^SOURCE_PREFIX=.*#SOURCE_PREFIX=202607/#' .env    # macOS sed

python copy_logs.py 2>&1 | tee /tmp/copy_202607.log
```

Watch progress in another shell (optional helper):

```bash
# reports overall % from the copy log + dest bucket (edit the log path/sizes inside)
python scripts/copy_status.py
```

## 3. Wait for auto-processing, then verify the Parquet output

Each copied file is auto-converted to one Parquet in the processed bucket.
Confirm **exactly one whole-file parquet per region** (4 total):

```bash
AWS_ACCESS_KEY_ID="$DEST_AWS_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$DEST_AWS_SECRET_ACCESS_KEY" \
  aws s3 ls "s3://$PROCESSED_BUCKET/datacite-logs/year=2026/month=7/" --recursive \
  --region "$AWS_REGION" | grep parquet
# expect exactly:
#   .../region=ap-southeast-1/DataCite-access-202607-ap-southeast-1.parquet
#   .../region=eu-west-1/DataCite-access-202607-eu-west-1.parquet
#   .../region=us-east-1/DataCite-access-202607-us-east-1.parquet
#   .../region=us-west-2/DataCite-access-202607-us-west-2.parquet
```

If a region's parquet is **missing** (its auto-trigger failed), see
[Manual reprocess](#manual-reprocess-rare).

## 4. Register the new partitions in Athena and verify

```sql
MSCK REPAIR TABLE datacite.resolution_logs;

-- row counts per region
SELECT region, count(*) AS events
FROM datacite.resolution_logs
WHERE year=2026 AND month=7
GROUP BY region ORDER BY region;

-- sanity: outcome distribution (1=success, 100=not-found)
SELECT response_code, count(*) AS cnt
FROM datacite.resolution_logs
WHERE year=2026 AND month=7
GROUP BY response_code ORDER BY cnt DESC;
```

A healthy month totals a few hundred million events with a not-found (RC 100)
rate around **2–3%**. (Reference: Apr 2026 634.6M · May 445.2M · Jun 320.7M ·
Jul 290.0M; July RC-100 = 2.49%.)

The standard monthly queries are in
[`analysis/monthly_queries.ipynb`](analysis/monthly_queries.ipynb); the AI-bot
traffic study (referrer + user-agent signals, all months) is in
[`analysis/ai_bot_traffic_analysis.ipynb`](analysis/ai_bot_traffic_analysis.ipynb).

---

## Manual reprocess (rare)

You only need this if a region's **auto-trigger failed** (a `.gz` is in the
staging bucket but its Parquet never appeared). Pick one:

**Option A — re-fire the S3 event by re-copying (simplest).** The event fires on
object creation, and `copy_logs.py` skips files already present, so delete the
staging object first, then re-copy:

```bash
set -a; . ./.env; set +a
KEY="202607/DataCite-access.log-202607-us-east-1.gz"     # the failed region
AWS_ACCESS_KEY_ID="$DEST_AWS_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$DEST_AWS_SECRET_ACCESS_KEY" \
  aws s3 rm "s3://$DEST_BUCKET/$KEY" --region "$AWS_REGION"
python copy_logs.py        # re-copies the missing file, re-triggering processing
```

**Option B — run one processor task directly** on the whole file (no re-copy),
using the same container the auto-trigger uses (`ECS_TASK_DEF`):

```bash
aws ecs run-task --cluster "$ECS_CLUSTER" --task-definition "$ECS_TASK_DEF" \
  --launch-type FARGATE --region "$AWS_REGION" \
  --network-configuration "awsvpcConfiguration={subnets=[$ECS_SUBNET],assignPublicIp=ENABLED}" \
  --overrides '{"containerOverrides":[{"name":"log-processor","environment":[
      {"name":"INPUT_BUCKET","value":"'"$DEST_BUCKET"'"},
      {"name":"INPUT_KEY","value":"'"$KEY"'"}]}]}'
```

Both produce the single correct `region=<region>/…-<region>.parquet`. Then run
`MSCK REPAIR TABLE` (step 4).

## Key AWS resources

| Resource | Value |
|---|---|
| Source bucket | `raw-resolution-logs.datacite.org` (DataCite account) |
| Staging bucket | `datacite-logs` (us-east-2) — **copy here to trigger processing** |
| Processed bucket | `datacite-logs-processed` (us-east-2) — Parquet output |
| Athena DB / table | `datacite.resolution_logs` |
| ECS cluster / task def | `datacite-logs` / `datacite-log-processor` |

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Copy log shows `Connection reset` / `Read timeout`, then continues | Normal — the flaky cross-account link; `copy_logs.py` retries and recovers. |
| Copy "stuck" at a % for 1–2 min | A read-timeout retry window (120s). It resumes; check the log tail for a new `part N` line. |
| Athena `WHERE year=Y AND month=M` returns empty after copy | You didn't run `MSCK REPAIR TABLE` yet (this table uses manual partition registration, not projection). |
| A region's parquet never appeared after copy | That region's auto-trigger failed → [Manual reprocess](#manual-reprocess-rare). |
| `copy_logs.py` KeyError on `*_AWS_ACCESS_KEY_ID` | `.env` missing/incomplete; ensure both SOURCE_ and DEST_ credential sets are set. |

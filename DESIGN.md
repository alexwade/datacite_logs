# DataCite Resolution Logs — Pipeline Design

## Overview

Monthly DOI resolution logs (~500M–700M events/month) are ingested from a source S3 bucket, converted to Snappy-compressed Parquet, and loaded into an Athena data warehouse for ad-hoc SQL queries.

```
Local machine
        │
        │  copy_logs.py        — cross-account multipart copy, 25 MB chunks
        ▼
s3://datacite-logs/YYYYMM/     ← raw .gz files, one per region
        │
        │  S3 ObjectCreated event  →  auto-triggers one processor per file
        ▼
Fargate processor task (per file)  ← runs inside AWS, same region as S3
        │  runner.py → log_processor.py — streams the .gz line-by-line → one Parquet
        ▼
s3://datacite-logs-processed/datacite-logs/year=YYYY/month=M/region=<region>/
        │                      ← Hive-partitioned Parquet, Snappy compressed,
        │                        one whole-file parquet per region
        ▼
AWS Athena  datacite.resolution_logs   (MSCK REPAIR TABLE to register partitions)
```

The copy is the only manual step; processing is **auto-triggered** by the S3
object-created event. See [RUNBOOK.md](RUNBOOK.md) for the monthly procedure and
the (rare) manual-reprocess path.

---

## Repository Contents

| File | Purpose |
|---|---|
| `copy_logs.py` | Cross-account S3 copy. Reads from source account, writes to `datacite-logs`. 25 MB chunks with exponential backoff retry; idempotent (skips files already present). |
| `lambda/log_processor.py` | Core processing logic. Streams gzip line-by-line, writes a single valid Parquet file to S3 via `_S3StreamingBuffer`. Invoked per file by the S3-event auto-trigger. |
| `lambda/runner.py` | Processor Fargate entry point. Reads `INPUT_BUCKET`, `INPUT_KEY`, `OUTPUT_KEY`, `OUTPUT_BUCKET` from env vars and calls `process()`. |
| `lambda/Dockerfile` | `python:3.12-slim` image with pyarrow and boto3. Build for `linux/amd64`, push to ECR. |
| `scripts/copy_status.py` | Reports copy progress % (from the copy log + dest bucket). |
| `lambda/requirements.txt` | Python dependencies: `pyarrow`, `boto3`. |

---

## AWS Resources

| Resource | Value |
|---|---|
| Source bucket | `raw-resolution-logs.datacite.org` |
| Staging bucket | `datacite-logs` (us-east-2) |
| Output bucket | `datacite-logs-processed` (us-east-2) |
| Comparison bucket | `datacite-logs-processed-compare` (us-east-2) |
| ECS Cluster | `datacite-logs` |
| Processor task definition | `datacite-log-processor` (current: `:4`) |
| Container name (processor) | `log-processor` |
| Athena database | `datacite` |
| Athena table | `resolution_logs` |
| Athena comparison table | `resolution_logs_compare` |
| Region | `us-east-2` |

---

## Configuration

Credentials and settings are loaded from a `.env` file. Copy `.env.example` to `.env` and fill in values:

```bash
cp .env.example .env
```

Install dependencies:

```bash
pip install boto3 python-dotenv
```

---

## Athena Schema

```sql
CREATE EXTERNAL TABLE datacite.resolution_logs (
  client_ip        string,
  protocol         string,
  ts               timestamp,
  request_count    int,
  response_code    smallint,
  duration_ms      int,
  doi              string,
  referrer_handle  string,
  referrer_url     string,
  user_agent       string
)
PARTITIONED BY (year int, month int, region string)
STORED AS PARQUET
LOCATION 's3://datacite-logs-processed/datacite-logs'
TBLPROPERTIES ('parquet.compress'='SNAPPY')
```

Always filter on `year`, `month`, and/or `region` to avoid full scans.

### Response codes

These are Handle System protocol codes, not HTTP status codes:

| Code | Meaning |
|---|---|
| 1 | Success — DOI resolved, URL returned |
| 2 | Error |
| 100 | Handle Not Found — DOI does not exist |
| 200 | Values Not Found — DOI exists but has no URL |

---

## Loaded Data

| Month | Region | Rows |
|---|---|---|
| 2026-04 | us-east-1 | 385,051,137 |
| 2026-04 | eu-west-1 | 91,466,794 |
| 2026-04 | us-west-2 | 82,636,023 |
| 2026-04 | ap-southeast-1 | 75,446,867 |
| | **Total** | **634,600,821** |
| 2026-05 | us-east-1 | 134,184,481 |
| 2026-05 | eu-west-1 | 86,736,063 |
| 2026-05 | us-west-2 | 81,471,598 |
| 2026-05 | ap-southeast-1 | 142,841,771 |
| | **Total** | **445,233,913** |

---

## Processing a New Monthly File

### 1. Copy raw logs to staging bucket

```bash
# Set SOURCE_PREFIX in .env to the new month (e.g. 202605/)
python copy_logs.py
```

### 2. Wait for auto-processing

Nothing to launch: the object-created event on `s3://datacite-logs/YYYYMM/`
auto-triggers one processor Fargate task per file, each streaming its `.gz` →
one whole-file Parquet in `datacite-logs-processed/…/region=<region>/`. Confirm
all four region parquets appear before continuing. (If a region's parquet never
shows up, its trigger failed — see [RUNBOOK.md](RUNBOOK.md) → Manual reprocess.)

**Deploy/update the processor image** (first time, and after any change to
`log_processor.py` / `runner.py`):

```bash
docker build --platform linux/amd64 -t datacite-log-processor lambda/
docker tag datacite-log-processor:latest <ECR_URI>:latest
docker push <ECR_URI>:latest
# The datacite-log-processor task definition's IAM role must allow:
#   s3:GetObject, s3:PutObject, s3:CreateMultipartUpload, s3:UploadPart,
#   s3:CompleteMultipartUpload, s3:AbortMultipartUpload
```

### 3. Register new Athena partitions

```sql
MSCK REPAIR TABLE datacite.resolution_logs;
```

This scans the S3 layout and registers any new `year=/month=/region=` partitions.
(You can still `ALTER TABLE … ADD PARTITION` a single partition by hand if needed.)

**Partition format notes:**
- Use `month=5` not `month=05` — Athena's INT partition type does not match leading-zero strings
- Use full region names: `us-east-1`, `eu-west-1`, `us-west-2`, `ap-southeast-1`

---

## Comparison Runs

To validate a pipeline change without overwriting production data, run a processor
task by hand with `OUTPUT_BUCKET` pointed at the comparison bucket (one `aws ecs
run-task` per file — see [RUNBOOK.md](RUNBOOK.md) → Manual reprocess, adding
`{"name":"OUTPUT_BUCKET","value":"datacite-logs-processed-compare"}` to the
container environment).

Create the comparison Athena table once:

```sql
CREATE EXTERNAL TABLE datacite.resolution_logs_compare (
  client_ip        string,
  protocol         string,
  ts               timestamp,
  request_count    int,
  response_code    smallint,
  duration_ms      int,
  doi              string,
  referrer_handle  string,
  referrer_url     string,
  user_agent       string
)
PARTITIONED BY (year int, month int, region string)
STORED AS PARQUET
LOCATION 's3://datacite-logs-processed-compare/datacite-logs'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
```

Register partitions after each comparison run (same `ALTER TABLE ADD PARTITION` pattern as the main table, pointing to the compare bucket). Then diff against production:

```sql
SELECT
  'production'  AS run, COUNT(*) AS rows FROM datacite.resolution_logs    WHERE year=2026 AND month=5
UNION ALL
SELECT
  'compare'     AS run, COUNT(*) AS rows FROM datacite.resolution_logs_compare WHERE year=2026 AND month=5;
```

---

## Key Implementation Notes

### `_S3StreamingBuffer`

The critical class in `log_processor.py`. PyArrow's `ParquetWriter` requires a single seekable-like sink. This class implements `io.RawIOBase` with `write()` and `tell()`, forwarding data to S3 via multipart upload (5 MB minimum part size). **One `ParquetWriter` → one buffer → one valid Parquet file.** Earlier approaches using multiple `ParquetWriter` instances (one per S3 part) produced multiple PAR1 headers concatenated together — valid as a byte stream but invalid Parquet.

### Region extraction

Filename pattern: `DataCite-access.log-YYYYMM-<region>.gz`. Split on `-`, join from index 3 to recover the full region name (e.g. `ap-southeast-1`, not `southeast-1`).

### Chunk output keys

The processor derives its `OUTPUT_KEY` from the input key's region and month, so
each region's Parquet lands in the correct `year=/month=/region=` Hive partition.
`OUTPUT_KEY` / `OUTPUT_BUCKET` env vars can override this (e.g. for comparison runs).

---

## Future Work

### Automated partition registration

After tasks complete, an EventBridge rule or Lambda could automatically call `ALTER TABLE ADD PARTITION` rather than requiring a manual step.

### Partition projection

Replace explicit `ALTER TABLE ADD PARTITION` with [Athena partition projection](https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html) on `year`, `month`, and `region` — eliminates the manual partition registration step entirely for new monthly files.

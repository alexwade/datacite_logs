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
        │  chunk_and_process.py — launches 4 splitter Fargate tasks (one per region)
        ▼
Fargate splitter tasks ×4      ← run inside AWS, same region as S3
        │  splitter.py         — streams .gz, splits into 8 chunks, uploads back to S3,
        │                        launches 8 processor tasks, waits for them
        ▼
Fargate processor tasks ×32    ← 4 regions × 8 chunks × 2 vCPU = 64 vCPU
        │  log_processor.py    — streams chunk line-by-line → Parquet
        ▼
s3://datacite-logs-processed/datacite-logs/year=YYYY/month=M/region=<region>/
        │                      ← Hive-partitioned Parquet, Snappy compressed
        ▼
AWS Athena  datacite.resolution_logs
```

---

## Repository Contents

| File | Purpose |
|---|---|
| `copy_logs.py` | Cross-account S3 copy. Reads from source account, writes to `datacite-logs`. 25 MB chunks with exponential backoff retry. |
| `chunk_and_process.py` | Local launcher. Launches one splitter Fargate task per region and waits for them to complete. No data passes through the local machine. |
| `lambda/splitter.py` | Fargate splitter task. Streams a `.gz` from S3, round-robin splits into N gzip chunks, uploads them, launches N processor tasks, and waits. Runs inside AWS. |
| `lambda/log_processor.py` | Core processing logic. Streams gzip line-by-line, writes a single valid Parquet file to S3 via `_S3StreamingBuffer`. |
| `lambda/runner.py` | Processor Fargate entry point. Reads `INPUT_BUCKET`, `INPUT_KEY`, `OUTPUT_KEY`, `OUTPUT_BUCKET` from env vars and calls `process()`. |
| `lambda/Dockerfile` | `python:3.12-slim` image with pyarrow and boto3. Shared by both splitter and processor tasks. Build for `linux/amd64`, push to ECR. |
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
| Splitter task definition | `datacite-log-splitter` (current: `:1`) |
| Container name (processor) | `log-processor` |
| Container name (splitter) | `log-splitter` |
| Fargate vCPU quota | 64 vCPU on-demand (us-east-2) |
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

---

## Processing a New Monthly File

### 1. Copy raw logs to staging bucket

```bash
# Set SOURCE_PREFIX in .env to the new month (e.g. 202605/)
python copy_logs.py
```

### 2. Launch splitter tasks

```bash
python chunk_and_process.py
# single file:
python chunk_and_process.py --key 202605/DataCite-access.log-202605-us-east-1.gz
```

This launches 4 splitter Fargate tasks (one per region) and waits for them. No data passes through the local machine.

Each splitter task runs inside AWS and:
- Streams the `.gz` directly from S3 (same-region, fast)
- Round-robin splits lines into 8 gzip chunks, uploading back to S3 via multipart
- Launches 8 processor tasks in parallel
- Waits for all 8 to complete before exiting

All 32 processor tasks (4 regions × 8 chunks × 2 vCPU = 64 vCPU) run simultaneously. The largest file (~1.8 GB compressed, 385M rows) completes in ~6 min end-to-end. The splitter task itself requires ~2 vCPU and runs for the duration of splitting + processing.

**Deploy the splitter image** (first time, and after any changes to `splitter.py`):

```bash
# Build and push — same image as the processor, both entry points included
docker build --platform linux/amd64 -t datacite-log-processor lambda/
docker tag datacite-log-processor:latest <ECR_URI>:latest
docker push <ECR_URI>:latest

# Register the splitter task definition (CMD override to run splitter.py)
# In the ECS console or via CLI, create datacite-log-splitter with:
#   image: <ECR_URI>:latest
#   command: ["python", "splitter.py"]
#   container name: log-splitter
#   IAM role must allow: s3:GetObject, s3:PutObject, s3:CreateMultipartUpload,
#                        s3:UploadPart, s3:CompleteMultipartUpload,
#                        s3:AbortMultipartUpload, ecs:RunTask, iam:PassRole
```

### 3. Register new Athena partitions

```sql
ALTER TABLE datacite.resolution_logs ADD PARTITION
  (year=2026, month=5, region='us-east-1')
  LOCATION 's3://datacite-logs-processed/datacite-logs/year=2026/month=5/region=us-east-1/';
-- repeat for each region
```

**Partition format notes:**
- Use `month=5` not `month=05` — Athena's INT partition type does not match leading-zero strings
- Use full region names: `us-east-1`, `eu-west-1`, `us-west-2`, `ap-southeast-1`

---

## Comparison Runs

To validate a pipeline change without overwriting production data, direct output to the comparison bucket:

```bash
python chunk_and_process.py --output-bucket datacite-logs-processed-compare
```

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

`chunk_and_process.py` constructs the `OUTPUT_KEY` before launching each Fargate task and passes it as an environment variable. This ensures all chunks for a region land in the same Hive partition directory with distinct filenames.

---

## Future Work

### Automated partition registration

After tasks complete, an EventBridge rule or Lambda could automatically call `ALTER TABLE ADD PARTITION` rather than requiring a manual step.

### Partition projection

Replace explicit `ALTER TABLE ADD PARTITION` with [Athena partition projection](https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html) on `year`, `month`, and `region` — eliminates the manual partition registration step entirely for new monthly files.

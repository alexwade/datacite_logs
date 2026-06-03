# DataCite Resolution Logs — Pipeline Design

## Overview

Monthly DOI resolution logs (~500M–700M events/month) are ingested from a source S3 bucket, converted to Snappy-compressed Parquet, and loaded into an Athena data warehouse for ad-hoc SQL queries.

```
raw-resolution-logs.datacite.org  (source account)
        │
        │  copy_logs.py  — cross-account multipart copy, 25 MB chunks
        ▼
s3://datacite-logs/YYYYMM/        ← raw .gz files, one per region
        │
        │  AWS ECS Fargate task    ← triggered by S3 Event → EventBridge
        │  log_processor.py        ← streams gzip line-by-line → Parquet
        ▼
s3://datacite-logs-processed/datacite-logs/year=YYYY/month=M/region=<region>/
        │                          ← Hive-partitioned Parquet, Snappy compressed
        ▼
AWS Athena  datacite.resolution_logs
```

---

## Repository Contents

| File | Purpose |
|---|---|
| `copy_logs.py` | Cross-account S3 copy. Reads from source account, writes to `datacite-logs`. 25 MB chunks with exponential backoff retry. |
| `chunk_and_process.py` | Splits each `.gz` into N gzip chunks via round-robin line distribution, uploads chunks to S3, launches N parallel Fargate tasks. See [Future Work](#future-work). |
| `lambda/log_processor.py` | Core processing logic. Streams gzip line-by-line, writes a single valid Parquet file to S3 via `_S3StreamingBuffer`. |
| `lambda/runner.py` | Fargate container entry point. Reads `INPUT_BUCKET`, `INPUT_KEY`, `OUTPUT_KEY` from env vars and calls `process()`. |
| `lambda/Dockerfile` | `python:3.12-slim` image with pyarrow. Build for `linux/amd64`, push to ECR. |
| `lambda/requirements.txt` | Python dependencies: `pyarrow`, `boto3`. |

---

## AWS Resources

| Resource | Value |
|---|---|
| Source bucket | `raw-resolution-logs.datacite.org` |
| Staging bucket | `datacite-logs` (us-east-2) |
| Output bucket | `datacite-logs-processed` (us-east-2) |
| ECS Cluster | `datacite-logs` |
| Task definition | `datacite-log-processor` (current: `:4`) |
| Container name | `log-processor` |
| Athena database | `datacite` |
| Athena table | `resolution_logs` |
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

### 2. Trigger Fargate processing

An S3 Event Notification on `datacite-logs` fires an EventBridge rule that launches one Fargate task per uploaded `.gz` file. The task:

- Streams gzip decompression line-by-line (peak memory ~50–100 MB regardless of file size)
- Writes a single valid Parquet file to the processed bucket via `_S3StreamingBuffer`
- Runs to completion with no timeout — large files (~1.8 GB compressed, 385M rows) take ~50 minutes

No Lambda, no queue. One task, one file, one Parquet output.

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

## Key Implementation Notes

### `_S3StreamingBuffer`

The critical class in `log_processor.py`. PyArrow's `ParquetWriter` requires a single seekable-like sink. This class implements `io.RawIOBase` with `write()` and `tell()`, forwarding data to S3 via multipart upload (5 MB minimum part size). **One `ParquetWriter` → one buffer → one valid Parquet file.** Earlier approaches using multiple `ParquetWriter` instances (one per S3 part) produced multiple PAR1 headers concatenated together — valid as a byte stream but invalid Parquet.

### Region extraction

Filename pattern: `DataCite-access.log-YYYYMM-<region>.gz`. Split on `-`, join from index 3 to recover the full region name (e.g. `ap-southeast-1`, not `southeast-1`).

### Chunk output keys

`chunk_and_process.py` constructs the `OUTPUT_KEY` before launching each Fargate task and passes it as an environment variable. This ensures all chunks for a region land in the same Hive partition directory with distinct filenames.

---

## Future Work

### Chunked parallel processing

For large files (us-east-1 at 385M rows takes ~50 min), splitting each `.gz` into N chunks and processing them in parallel across N Fargate tasks reduces wall-clock time proportionally.

`chunk_and_process.py` implements this today and can be run manually:

```bash
python chunk_and_process.py --chunks 8
# single file:
python chunk_and_process.py --chunks 8 --key 202605/DataCite-access.log-202605-us-east-1.gz
```

**How it works:**

```
s3://datacite-logs/YYYYMM/<file>.gz
        │
        │  chunk_and_process.py
        │  streams .gz, round-robins lines into N gzip chunk files
        │  uploads chunks → s3://datacite-logs/YYYYMM/chunks/
        │  launches N Fargate tasks in parallel
        │
        ├── Fargate task 0  →  <file>-chunk-000.parquet
        ├── Fargate task 1  →  <file>-chunk-001.parquet
        └── Fargate task N  →  <file>-chunk-00N.parquet
                │
                │  all land in the same Hive partition directory
                ▼
s3://datacite-logs-processed/datacite-logs/year=YYYY/month=M/region=<region>/
```

**Blocker:** The default Fargate vCPU quota in us-east-2 is 2 vCPU (1 task at 2 vCPU). A quota increase to 32 vCPU has been requested, which would allow 8–16 parallel tasks per file. Once approved, target configuration is 8 chunks per file (~6 min end-to-end for the largest files).

### Automated partition registration

After tasks complete, an EventBridge rule or Lambda could automatically call `ALTER TABLE ADD PARTITION` rather than requiring a manual step.

### Partition projection

Replace explicit `ALTER TABLE ADD PARTITION` with [Athena partition projection](https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html) on `year`, `month`, and `region` — eliminates the manual partition registration step entirely for new monthly files.

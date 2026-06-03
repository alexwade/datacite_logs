# DataCite Resolution Logs Pipeline

Scripts for ingesting DataCite DOI resolution logs from S3, converting them to Parquet, and querying them via AWS Athena.

Processes ~500M–700M resolution events per month across 4 AWS regions into a Hive-partitioned data warehouse.

## Design

See [DESIGN.md](DESIGN.md) for full architecture, schema, processing instructions, and future work.

## Quick Start

```bash
cp .env.example .env
# fill in credentials in .env

pip install boto3 python-dotenv
```

**Copy raw logs from source bucket:**
```bash
python copy_logs.py
```

**Process a new month (triggered automatically via S3 Event → EventBridge → Fargate, or run manually):**
```bash
# chunk_and_process.py for parallel processing (see DESIGN.md Future Work)
python chunk_and_process.py --chunks 4 --key 202605/DataCite-access.log-202605-us-east-1.gz
```

## Repository Structure

```
.
├── copy_logs.py            # Cross-account S3 copy (source → staging bucket)
├── chunk_and_process.py    # Split .gz into chunks, launch parallel Fargate tasks
├── lambda/
│   ├── log_processor.py    # Core processing: gzip → Parquet via streaming S3 writer
│   ├── runner.py           # Fargate container entry point
│   ├── Dockerfile          # python:3.12-slim image, push to ECR
│   └── requirements.txt
├── .env.example            # Required environment variables
└── DESIGN.md               # Full architecture and design doc
```

## Athena Quick Reference

```sql
-- Row counts by region
SELECT region, count(*) AS rows
FROM datacite.resolution_logs
WHERE year=2026 AND month=4
GROUP BY region ORDER BY rows DESC;

-- Unique successfully-resolved DOIs
SELECT count(DISTINCT doi) AS unique_dois
FROM datacite.resolution_logs
WHERE year=2026 AND month=4 AND response_code = 1;

-- Top referrers
SELECT referrer_url, count(*) AS cnt
FROM datacite.resolution_logs
WHERE year=2026 AND month=4
GROUP BY referrer_url ORDER BY cnt DESC LIMIT 20;
```

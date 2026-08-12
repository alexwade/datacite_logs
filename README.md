# DataCite Resolution Logs Pipeline

Ingests DataCite DOI resolution logs from S3, converts them to Parquet, and makes
them queryable via AWS Athena. Processes a few hundred million resolution events
per month across 4 AWS regions into a Hive-partitioned data warehouse
(`datacite.resolution_logs`).

- **[RUNBOOK.md](RUNBOOK.md)** — **start here each month.** Step-by-step
  operational guide (copy → auto-process → register in Athena → verify), plus
  recovery and troubleshooting.
- **[DESIGN.md](DESIGN.md)** — architecture, schema, and design rationale.

> **The one thing to remember:** copying a month's files into
> `s3://datacite-logs/YYYYMM/` **auto-triggers** processing (S3 event → Fargate).
> Each `.gz` becomes one Parquet on its own — so the monthly job is just
> **copy → wait → `MSCK REPAIR TABLE`**.

## Quick start

```bash
cp .env.example .env         # fill in SOURCE_* and DEST_* credentials
pip install boto3 python-dotenv pyathena pandas

# 1. point at the month and copy (auto-triggers processing on arrival)
#    edit SOURCE_PREFIX=YYYYMM/ in .env, then:
python copy_logs.py

# 2. after the 4 region parquets appear, register + verify in Athena
#    MSCK REPAIR TABLE datacite.resolution_logs;   (see RUNBOOK step 4)
```

Full details, verification queries, and gotchas are in **[RUNBOOK.md](RUNBOOK.md)**.

## Repository structure

```
.
├── RUNBOOK.md                 # Monthly operational guide — READ FIRST
├── DESIGN.md                  # Architecture, schema, rationale
├── copy_logs.py               # Cross-account S3 copy (source → staging); idempotent, retrying
├── lambda/                    # The auto-triggered processor
│   ├── log_processor.py       #   gzip → Parquet via streaming S3 writer
│   ├── runner.py              #   Fargate container entry point
│   ├── Dockerfile             #   python:3.12-slim image (push to ECR)
│   └── requirements.txt
├── scripts/
│   └── copy_status.py         #   report copy progress %
├── analysis/
│   ├── monthly_queries.ipynb  #   standard per-month queries (regions, outcomes, referrers, UAs)
│   └── ai_bot_traffic_analysis.ipynb  # AI-bot traffic study (referrer + user-agent signals)
├── .env.example               # Required environment variables
└── LICENSE
```

## Athena quick reference

```sql
-- Row counts by region for a month
SELECT region, count(*) AS rows
FROM datacite.resolution_logs
WHERE year=2026 AND month=7
GROUP BY region ORDER BY rows DESC;

-- Outcome mix (1=success, 100=DOI-not-found)
SELECT response_code, count(*) AS cnt
FROM datacite.resolution_logs
WHERE year=2026 AND month=7
GROUP BY response_code ORDER BY cnt DESC;

-- Unique successfully-resolved DOIs
SELECT count(DISTINCT doi) AS unique_dois
FROM datacite.resolution_logs
WHERE year=2026 AND month=7 AND response_code = 1;
```

## Data processed so far

| Month | Events | Not-found (RC 100) |
|---|---|---|
| Apr 2026 | 634.6M | — |
| May 2026 | 445.2M | — |
| Jun 2026 | 320.7M | — |
| Jul 2026 | 290.0M | 2.49% |

## Notes

AWS credentials live only in your local `.env` (gitignored) 

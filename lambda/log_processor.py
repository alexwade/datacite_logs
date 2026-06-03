"""
DataCite resolution log processor.
Streams gzip decompression line-by-line, writing a single valid Parquet file
to S3 via multipart upload. Peak memory is ~50-100 MB regardless of file size.

Environment variables:
  OUTPUT_BUCKET  — S3 bucket name for Parquet output
"""

import boto3
import gzip
import io
import os
import re
import urllib.parse
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
ROW_GROUP_SIZE = 500_000
MIN_PART_BYTES = 5 * 1024 * 1024   # S3 minimum multipart part size

LOG_RE = re.compile(
    r'^(\S+)\s+(\S+)\s+"([^"]+)"\s+(\d+)\s+(\d+)\s+(\d+)ms\s+'
    r'(\S+)\s+"([^"]*)"\s+"([^"]*)"\s+"([^"]*)"'
)

SCHEMA = pa.schema([
    ("client_ip",       pa.string()),
    ("protocol",        pa.string()),
    ("ts",              pa.timestamp("ms", tz="UTC")),
    ("request_count",   pa.int32()),
    ("response_code",   pa.int16()),
    ("duration_ms",     pa.int32()),
    ("doi",             pa.string()),
    ("referrer_handle", pa.string()),
    ("referrer_url",    pa.string()),
    ("user_agent",      pa.string()),
    ("year",            pa.int16()),
    ("month",           pa.int8()),
    ("region",          pa.string()),
])


def _extract_region(key: str) -> str:
    # Filename: DataCite-access.log-YYYYMM-<region>.gz
    # Split on '-' gives: ['DataCite', 'access.log', 'YYYYMM', '<geo>', '<dir>', '<num>']
    # Rejoin from index 3 to recover the full region string e.g. ap-southeast-1
    name = key.split("/")[-1].replace(".gz", "")
    parts = name.split("-")
    return "-".join(parts[3:]) if len(parts) >= 4 else "unknown"


def _output_key(key: str, region: str, year: int, month: int) -> str:
    stem = key.split("/")[-1].replace(".gz", "").replace(".log", "")
    return f"datacite-logs/year={year}/month={month}/region={region}/{stem}.parquet"


def _empty_batch() -> dict:
    return {name: [] for name in SCHEMA.names}


class _S3StreamingBuffer(io.RawIOBase):
    """Write-only stream that forwards data to S3 via multipart upload.

    Implements write() and tell() so PyArrow's ParquetWriter can use it
    as a single file sink — producing one valid Parquet file regardless of size.
    """

    def __init__(self, s3_client, bucket: str, key: str):
        self._s3 = s3_client
        self._bucket = bucket
        self._key = key
        self._buf = bytearray()
        self._pos = 0
        resp = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
        self._upload_id = resp["UploadId"]
        self._parts: list[dict] = []

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._pos

    def write(self, b) -> int:
        self._buf.extend(b)
        self._pos += len(b)
        while len(self._buf) >= MIN_PART_BYTES:
            self._upload_part(MIN_PART_BYTES)
        return len(b)

    def _upload_part(self, size: int) -> None:
        data = bytes(self._buf[:size])
        del self._buf[:size]
        part_num = len(self._parts) + 1
        resp = self._s3.upload_part(
            Bucket=self._bucket, Key=self._key,
            UploadId=self._upload_id, PartNumber=part_num, Body=data,
        )
        self._parts.append({"PartNumber": part_num, "ETag": resp["ETag"]})

    def complete(self) -> None:
        if self._buf:
            self._upload_part(len(self._buf))
        self._s3.complete_multipart_upload(
            Bucket=self._bucket, Key=self._key, UploadId=self._upload_id,
            MultipartUpload={"Parts": self._parts},
        )

    def abort(self) -> None:
        self._s3.abort_multipart_upload(
            Bucket=self._bucket, Key=self._key, UploadId=self._upload_id,
        )


def process(s3_client, input_bucket: str, key: str, output_key_override: str | None = None) -> None:
    region = _extract_region(key)
    streaming_body = s3_client.get_object(Bucket=input_bucket, Key=key)["Body"]

    batch = _empty_batch()
    year = month = None
    row_count = 0
    parse_errors = 0
    out_key = None
    stream = None
    writer = None

    try:
        with gzip.GzipFile(fileobj=streaming_body) as gz:
            for raw_line in io.TextIOWrapper(gz, encoding="utf-8", errors="replace"):
                m = LOG_RE.match(raw_line.rstrip())
                if not m:
                    parse_errors += 1
                    continue

                ts = datetime.strptime(m.group(3), "%Y-%m-%d %H:%M:%S.%fZ").replace(
                    tzinfo=timezone.utc
                )
                if year is None:
                    year, month = ts.year, ts.month
                    out_key = output_key_override or _output_key(key, region, year, month)
                    stream = _S3StreamingBuffer(s3_client, OUTPUT_BUCKET, out_key)
                    writer = pq.ParquetWriter(stream, SCHEMA, compression="snappy")

                batch["client_ip"].append(m.group(1))
                batch["protocol"].append(m.group(2))
                batch["ts"].append(ts)
                batch["request_count"].append(int(m.group(4)))
                batch["response_code"].append(int(m.group(5)))
                batch["duration_ms"].append(int(m.group(6)))
                batch["doi"].append(m.group(7))
                batch["referrer_handle"].append(m.group(8) or None)
                batch["referrer_url"].append(m.group(9) or None)
                batch["user_agent"].append(m.group(10) or None)
                batch["year"].append(ts.year)
                batch["month"].append(ts.month)
                batch["region"].append(region)
                row_count += 1

                if row_count % ROW_GROUP_SIZE == 0:
                    writer.write_table(pa.table(batch, schema=SCHEMA))
                    for v in batch.values():
                        v.clear()

        if writer and batch["doi"]:
            writer.write_table(pa.table(batch, schema=SCHEMA))

    except Exception:
        if stream:
            stream.abort()
        raise

    if row_count == 0:
        print(f"WARN: no rows parsed from s3://{input_bucket}/{key} "
              f"({parse_errors} parse errors) — skipping upload")
        return

    writer.close()
    stream.complete()
    print(
        f"OK s3://{OUTPUT_BUCKET}/{out_key} "
        f"rows={row_count} parse_errors={parse_errors}"
    )


def handler(event, context):
    s3 = boto3.client("s3")
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        process(s3, bucket, key)

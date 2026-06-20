"""Sales feed extractor (process #2).

Reads sales_oltp READ-ONLY (as sales_extract) and emits each table as CSV plus a
_manifest.json to s3://<landing>/sales/<run_date>/, advancing a per-table updated_at
high-watermark stored in MinIO (s3://<landing>/sales/_state/feed_watermark.json).

First run (no stored watermark) starts from epoch -> full history = onboarding load.
Later runs extract only updated_at >= last_watermark (inclusive; downstream MERGE is
idempotent) and <= run_hwm (a single NOW() captured per run). Config-driven, so the
same code runs on Lenovo (dev) or inside the airflow container (prod). See
docs/Sales_Feed_Design.md.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
from datetime import datetime

import boto3
from botocore.config import Config
from sqlalchemy import text

from sales_db import cfg, get_engine

TABLES = ["product_categories", "customers", "products", "stores", "orders", "order_items"]
EPOCH = "1970-01-01 00:00:00"
STATE_KEY = "sales/_state/feed_watermark.json"


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=cfg("MINIO_ENDPOINT"),
        aws_access_key_id=cfg("MINIO_ACCESS_KEY"),
        aws_secret_access_key=cfg("MINIO_SECRET_KEY"),
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def _ensure_bucket(s3, bucket: str) -> None:
    if bucket not in [b["Name"] for b in s3.list_buckets().get("Buckets", [])]:
        s3.create_bucket(Bucket=bucket)
        print(f"  created bucket: {bucket}")


def _read_state(s3, bucket: str) -> dict:
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=STATE_KEY)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {}


def _rows_to_csv(header, rows) -> bytes:
    buf = io.StringIO(newline="")
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


def extract(full: bool = False, run_date: str | None = None) -> None:
    bucket = cfg("LANDING_BUCKET", "landing")
    eng = get_engine("extract")
    s3 = _s3()
    _ensure_bucket(s3, bucket)
    state = {} if full else _read_state(s3, bucket)

    with eng.connect() as cx:
        run_hwm = cx.execute(text("SELECT NOW()")).scalar()  # single upper bound for the whole run
        # The prefix LABEL can be pinned (e.g. by the Airflow DAG, so bronze/silver/gold share one
        # run_date); the watermark window is unaffected. Defaults to this run's wall-clock date.
        run_date = run_date or run_hwm.date().isoformat()
        manifest_tables = []
        for t in TABLES:
            low = state.get(t, EPOCH)
            res = cx.execute(
                text(f"SELECT * FROM {t} WHERE updated_at >= :low AND updated_at <= :hwm ORDER BY updated_at"),
                {"low": low, "hwm": run_hwm})
            header = list(res.keys())
            rows = res.fetchall()
            key = f"sales/{run_date}/{t}_{run_date}.csv"
            s3.put_object(Bucket=bucket, Key=key, Body=_rows_to_csv(header, rows), ContentType="text/csv")
            manifest_tables.append({"table": t, "file": f"{t}_{run_date}.csv",
                                    "low_watermark": str(low), "rows": len(rows)})
            print(f"  {t:20} rows={len(rows):>7}  -> s3://{bucket}/{key}")

    manifest = {
        "source": "sales_oltp",
        "run_date": run_date,
        "extracted_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "run_hwm": str(run_hwm),
        "tables": manifest_tables,
    }
    s3.put_object(Bucket=bucket, Key=f"sales/{run_date}/_manifest.json",
                  Body=json.dumps(manifest, indent=2).encode(), ContentType="application/json")
    print(f"  manifest             -> s3://{bucket}/sales/{run_date}/_manifest.json")

    s3.put_object(Bucket=bucket, Key=STATE_KEY,
                  Body=json.dumps({t: str(run_hwm) for t in TABLES}, indent=2).encode(),
                  ContentType="application/json")
    print(f"  watermark advanced to {run_hwm}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Sales feed extractor -> MinIO landing")
    p.add_argument("--full", action="store_true", help="ignore stored watermark (re-extract everything)")
    p.add_argument("--run-date", default=None, help="pin the landing prefix label (YYYY-MM-DD); default = today")
    a = p.parse_args()
    extract(full=a.full, run_date=a.run_date)

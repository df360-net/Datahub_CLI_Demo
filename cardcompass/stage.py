from __future__ import annotations

import csv
import pathlib
from datetime import date as date_cls

from sqlalchemy import text
from sqlalchemy.engine import Engine

# Source columns present in the inbound CSVs (load_business_date + loaded_at
# are added at stage load: the former from the run date, the latter by the
# column's SYSUTCDATETIME() default).
AUTH_COLS = [
    "txn_id", "card_number_hash", "auth_ts", "auth_amount", "txn_currency",
    "merchant_id", "mcc", "txn_type", "response_code", "file_seq",
]
POST_COLS = [
    "txn_id", "posting_ref", "posting_ts", "settled_amount",
    "settlement_currency", "acquirer_id", "file_seq",
]


def _insert_sql(schema: str, table: str, cols: list[str]) -> str:
    collist = ", ".join(cols + ["load_business_date"])
    placeholders = ", ".join(f":{c}" for c in cols) + ", :d"
    return f"INSERT INTO {schema}.{table} ({collist}) VALUES ({placeholders})"


def _load_file(engine: Engine, schema: str, table: str, cols: list[str],
               csv_path: pathlib.Path, biz_date: date_cls) -> int:
    with csv_path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    d = str(biz_date)
    params = [{**{c: r[c] for c in cols}, "d": d} for r in rows]
    # Idempotent reset + load as one transaction; a failure leaves the date's
    # rows untouched.
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {schema}.{table} WHERE load_business_date = :d"), {"d": d})
        if params:
            conn.execute(text(_insert_sql(schema, table, cols)), params)
    return len(rows)


def load_stage(engine: Engine, biz_date: date_cls, inbound_dir: str) -> dict[str, int]:
    """Load both inbound files into CARD_STG for the business date.

    Raw landing: every column stored as nvarchar, no type coercion, duplicates
    accepted (DUPLICATE_FEED catches them downstream).
    """
    compact = biz_date.strftime("%Y%m%d")
    inbound = pathlib.Path(inbound_dir)
    auth_path = inbound / f"card_auth_{compact}.csv"
    post_path = inbound / f"card_post_{compact}.csv"
    for p in (auth_path, post_path):
        if not p.exists():
            raise FileNotFoundError(f"missing stage input: {p}")

    n_auth = _load_file(engine, "CARD_STG", "card_auth", AUTH_COLS, auth_path, biz_date)
    n_post = _load_file(engine, "CARD_STG", "card_post", POST_COLS, post_path, biz_date)
    return {"card_auth": n_auth, "card_post": n_post}

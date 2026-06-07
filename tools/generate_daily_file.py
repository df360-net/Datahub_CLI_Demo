"""Mock daily file generator for CardCompass.

Writes two CSVs for a business date, mimicking a payment processor's drop:
  data/inbound/card_auth_<YYYYMMDD>.csv  — every authorization attempt
  data/inbound/card_post_<YYYYMMDD>.csv  — settlements (subset of approved auths)

Deterministic for a given (--date, --seed): same inputs -> identical files, so
the daily ETL demo is reproducible. Card hashes + merchants are read from the
pre-loaded CARD_REF tables so the integration join populates.

    python tools/generate_daily_file.py --date 2026-06-07 --rows 500
"""
from __future__ import annotations

import argparse
import csv
import os
import pathlib
import random
import sys
from datetime import date as date_cls
from datetime import datetime, time, timedelta

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from sqlalchemy import text  # noqa: E402

from cardcompass.config import AppConfig  # noqa: E402
from cardcompass.db import make_engine  # noqa: E402

AUTH_COLS = [
    "txn_id", "card_number_hash", "auth_ts", "auth_amount", "txn_currency",
    "merchant_id", "mcc", "txn_type", "response_code", "file_seq",
]
POST_COLS = [
    "txn_id", "posting_ref", "posting_ts", "settled_amount",
    "settlement_currency", "acquirer_id", "file_seq",
]

DECLINE_CODES = ["05", "51", "14", "54", "61", "65", "91"]
TXN_TYPES = ["PURCHASE", "REFUND", "AUTH_ONLY"]
TXN_TYPE_WEIGHTS = [0.8, 0.1, 0.1]
CURRENCIES = ["USD", "USD", "USD", "USD", "EUR", "GBP", "CAD"]

DECLINE_RATE = 0.05
MALFORMED_RATE = 0.01
DUP_RATE = 0.001
POST_RATE = 0.98


def _load_reference(cfg: AppConfig):
    engine = make_engine(cfg)
    with engine.connect() as conn:
        # ORDER BY is load-bearing: rng.choice indexes by row order, so a stable
        # order is what makes the files reproducible for a given (date, seed).
        cards = [r[0] for r in conn.execute(
            text("SELECT card_number_hash FROM CARD_REF.card ORDER BY card_id"))]
        merchants = [
            (r[0], r[1])
            for r in conn.execute(
                text("SELECT merchant_id, mcc FROM CARD_REF.merchant ORDER BY merchant_id"))
        ]
    engine.dispose()
    if not cards or not merchants:
        raise SystemExit("CARD_REF.card / CARD_REF.merchant are empty — run db/02_seed_reference.sql")
    return cards, merchants


def _rand_ts(rng: random.Random, biz: date_cls) -> str:
    secs = rng.randint(0, 86399)
    return (datetime.combine(biz, time()) + timedelta(seconds=secs)).isoformat()


def _gen_auth_rows(rng: random.Random, biz: date_cls, rows: int, cards, merchants):
    out = []
    compact = biz.strftime("%Y%m%d")
    extra_seq = rows
    for i in range(rows):
        approved = rng.random() >= DECLINE_RATE
        merchant_id, mcc = rng.choice(merchants)
        amount = round(rng.uniform(1.0, 2000.0), 2)
        auth_ts = _rand_ts(rng, biz)
        txn_type = rng.choices(TXN_TYPES, weights=TXN_TYPE_WEIGHTS)[0]

        if rng.random() < MALFORMED_RATE:
            kind = rng.choice(["neg_amount", "future_date", "bad_mcc", "bad_type"])
            if kind == "neg_amount":
                amount = -amount
            elif kind == "future_date":
                auth_ts = (datetime.combine(biz, time()) + timedelta(days=30)).isoformat()
            elif kind == "bad_mcc":
                mcc = "AB12"
            else:
                txn_type = "BOGUS"

        row = {
            "txn_id": f"T{compact}{i:06d}",
            "card_number_hash": rng.choice(cards),
            "auth_ts": auth_ts,
            "auth_amount": f"{amount:.2f}",
            "txn_currency": rng.choice(CURRENCIES),
            "merchant_id": merchant_id,
            "mcc": mcc,
            "txn_type": txn_type,
            "response_code": "00" if approved else rng.choice(DECLINE_CODES),
            "file_seq": str(i + 1),
        }
        out.append((row, False))

        if rng.random() < DUP_RATE:
            extra_seq += 1
            dup = dict(row, file_seq=str(extra_seq))
            out.append((dup, True))
    return out


def _gen_post_rows(rng: random.Random, biz: date_cls, auth_rows):
    out = []
    compact = biz.strftime("%Y%m%d")
    k = 0
    for row, is_dup in auth_rows:
        if is_dup or row["response_code"] != "00":
            continue
        if rng.random() >= POST_RATE:
            continue  # ~2% straggle to the next day
        auth_amt = abs(float(row["auth_amount"]))
        settled = round(auth_amt * rng.uniform(1.0, 1.05), 2)
        posting_dt = datetime.fromisoformat(row["auth_ts"]) + timedelta(hours=rng.randint(2, 36))
        out.append({
            "txn_id": row["txn_id"],
            "posting_ref": f"P{compact}{k:06d}",
            "posting_ts": posting_dt.isoformat(),
            "settled_amount": f"{settled:.2f}",
            "settlement_currency": row["txn_currency"],
            "acquirer_id": f"A{rng.randint(1, 9):03d}",
            "file_seq": str(k + 1),
        })
        k += 1
    return out


def _write_csv(path: pathlib.Path, cols, rows):
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate mock CardCompass daily files")
    ap.add_argument("--date", required=True, help="business date, YYYY-MM-DD")
    ap.add_argument("--rows", type=int, default=500, help="auth rows (default 500)")
    ap.add_argument("--seed", default="42", help="RNG seed (default 42)")
    ap.add_argument("--inbound-dir", default=None, help="override INBOUND_DIR")
    args = ap.parse_args()

    biz = date_cls.fromisoformat(args.date)
    cfg = AppConfig.from_env()
    inbound = pathlib.Path(args.inbound_dir or cfg.inbound_dir)
    inbound.mkdir(parents=True, exist_ok=True)

    rng = random.Random(f"{args.seed}:{args.date}")
    cards, merchants = _load_reference(cfg)

    auth_rows = _gen_auth_rows(rng, biz, args.rows, cards, merchants)
    post_rows = _gen_post_rows(rng, biz, auth_rows)

    compact = biz.strftime("%Y%m%d")
    auth_path = inbound / f"card_auth_{compact}.csv"
    post_path = inbound / f"card_post_{compact}.csv"
    _write_csv(auth_path, AUTH_COLS, [r for r, _ in auth_rows])
    _write_csv(post_path, POST_COLS, post_rows)

    dups = sum(1 for _, is_dup in auth_rows if is_dup)
    declines = sum(1 for r, _ in auth_rows if r["response_code"] != "00")
    print(f"{auth_path}  ({len(auth_rows)} rows: {declines} declines, {dups} duplicates)")
    print(f"{post_path}  ({len(post_rows)} rows)")


if __name__ == "__main__":
    main()

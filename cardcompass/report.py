from __future__ import annotations

from datetime import date as date_cls

from sqlalchemy import text
from sqlalchemy.engine import Engine

# One row per (txn_date, mcc_category, txn_type). txn_count covers all
# transactions in the slice; the amount totals are approved-only (settled is
# NULL for unposted rows, hence COALESCE).
_DAILY_SUMMARY_INSERT = """
INSERT INTO CARD_RPT.daily_card_summary
    (txn_date, mcc_category, txn_type, txn_count, approved_count, decline_count,
     auth_amount_total, settled_amount_total)
SELECT
    txn_date,
    mcc_category,
    txn_type,
    COUNT(*),
    SUM(CAST(is_approved AS int)),
    SUM(CASE WHEN is_approved = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_approved = 1 THEN auth_amount ELSE 0 END),
    SUM(CASE WHEN is_approved = 1 THEN COALESCE(settled_amount, 0) ELSE 0 END)
FROM   CARD_INT.card_txn
WHERE  txn_date = :d
GROUP BY txn_date, mcc_category, txn_type
"""

# Top 10 merchants per day by approved settled volume. merchant_id breaks ties
# for deterministic ranking. [rank] is bracketed (reserved keyword).
_TOP_MERCHANTS_INSERT = """
INSERT INTO CARD_RPT.card_top_merchants
    (txn_date, [rank], merchant_id, merchant_name, txn_count, settled_amount_total)
SELECT txn_date, rnk, merchant_id, merchant_name, txn_count, settled_amount_total
FROM (
    SELECT
        txn_date,
        merchant_id,
        merchant_name,
        COUNT(*) AS txn_count,
        SUM(COALESCE(settled_amount, 0)) AS settled_amount_total,
        ROW_NUMBER() OVER (
            PARTITION BY txn_date
            ORDER BY SUM(COALESCE(settled_amount, 0)) DESC, merchant_id
        ) AS rnk
    FROM   CARD_INT.card_txn
    WHERE  txn_date = :d AND is_approved = 1
    GROUP BY txn_date, merchant_id, merchant_name
) s
WHERE s.rnk <= 10
"""


def load_report(engine: Engine, biz_date: date_cls) -> dict[str, int]:
    """Build CARD_RPT.daily_card_summary + card_top_merchants for the date."""
    d = str(biz_date)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM CARD_RPT.daily_card_summary WHERE txn_date = :d"), {"d": d})
        conn.execute(text("DELETE FROM CARD_RPT.card_top_merchants WHERE txn_date = :d"), {"d": d})
        conn.execute(text(_DAILY_SUMMARY_INSERT), {"d": d})
        conn.execute(text(_TOP_MERCHANTS_INSERT), {"d": d})
        n_sum = conn.execute(
            text("SELECT COUNT(*) FROM CARD_RPT.daily_card_summary WHERE txn_date = :d"), {"d": d}
        ).scalar_one()
        n_top = conn.execute(
            text("SELECT COUNT(*) FROM CARD_RPT.card_top_merchants WHERE txn_date = :d"), {"d": d}
        ).scalar_one()
    return {"daily_card_summary": n_sum, "card_top_merchants": n_top}

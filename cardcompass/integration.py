from __future__ import annotations

from datetime import date as date_cls

from sqlalchemy import text
from sqlalchemy.engine import Engine

# auth ⋈ post ⋈ ref. Inner joins to the reference tables drop rows with an
# unknown merchant or MCC (the generator's bad-MCC malformed rows fall out
# here). The WHERE filters drop the other malformed shapes (non-positive /
# unparseable amount, unparseable or future auth_ts, invalid txn_type).
# ROW_NUMBER de-dupes the raw feed's repeated txn_ids down to one row.
_CARD_TXN_INSERT = """
INSERT INTO CARD_INT.card_txn
    (txn_id, card_id, auth_ts, posting_ts, txn_date, auth_amount, settled_amount,
     txn_currency, merchant_id, merchant_name, mcc, mcc_category, txn_type,
     is_approved, is_posted, decline_reason)
SELECT txn_id, card_id, auth_ts, posting_ts, txn_date, auth_amount, settled_amount,
       txn_currency, merchant_id, merchant_name, mcc, mcc_category, txn_type,
       is_approved, is_posted, decline_reason
FROM (
    SELECT
        a.txn_id,
        c.card_id,
        TRY_CAST(a.auth_ts AS datetime2(3))        AS auth_ts,
        TRY_CAST(p.posting_ts AS datetime2(3))     AS posting_ts,
        a.load_business_date                       AS txn_date,
        TRY_CAST(a.auth_amount AS decimal(18,2))   AS auth_amount,
        TRY_CAST(p.settled_amount AS decimal(18,2)) AS settled_amount,
        a.txn_currency,
        a.merchant_id,
        m.merchant_name,
        a.mcc,
        mc.mcc_category,
        a.txn_type,
        CASE WHEN a.response_code = '00' THEN 1 ELSE 0 END AS is_approved,
        CASE WHEN p.txn_id IS NOT NULL THEN 1 ELSE 0 END   AS is_posted,
        CASE a.response_code
            WHEN '00' THEN NULL
            WHEN '05' THEN 'Do not honor'
            WHEN '51' THEN 'Insufficient funds'
            WHEN '14' THEN 'Invalid card number'
            WHEN '54' THEN 'Expired card'
            WHEN '61' THEN 'Exceeds withdrawal limit'
            WHEN '65' THEN 'Activity limit exceeded'
            WHEN '91' THEN 'Issuer unavailable'
            ELSE 'Declined'
        END AS decline_reason,
        ROW_NUMBER() OVER (PARTITION BY a.txn_id ORDER BY a.file_seq) AS rn
    FROM       CARD_STG.card_auth a
    JOIN       CARD_REF.card         c  ON c.card_number_hash = a.card_number_hash
    JOIN       CARD_REF.merchant     m  ON m.merchant_id      = a.merchant_id
    JOIN       CARD_REF.mcc_category mc ON mc.mcc             = a.mcc
    LEFT JOIN  CARD_STG.card_post    p  ON p.txn_id           = a.txn_id
                                       AND p.load_business_date = a.load_business_date
    WHERE a.load_business_date = :d
      AND TRY_CAST(a.auth_amount AS decimal(18,2)) > 0
      AND TRY_CAST(a.auth_ts AS datetime2(3)) IS NOT NULL
      AND CAST(TRY_CAST(a.auth_ts AS datetime2(3)) AS date) <= a.load_business_date
      AND a.txn_type IN ('PURCHASE', 'REFUND', 'AUTH_ONLY')
) src
WHERE src.rn = 1
"""

_CARD_TXN_DECLINE_INSERT = """
INSERT INTO CARD_INT.card_txn_decline
    (txn_id, card_id, txn_date, auth_amount, decline_reason, flagged_for_review)
SELECT txn_id, card_id, txn_date, auth_amount, decline_reason,
       CASE WHEN auth_amount > 1000 THEN 1 ELSE 0 END
FROM   CARD_INT.card_txn
WHERE  txn_date = :d AND is_approved = 0
"""


def load_integration(engine: Engine, biz_date: date_cls) -> dict[str, int]:
    """Build CARD_INT.card_txn + card_txn_decline from staged rows for the date."""
    d = str(biz_date)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM CARD_INT.card_txn_decline WHERE txn_date = :d"), {"d": d})
        conn.execute(text("DELETE FROM CARD_INT.card_txn WHERE txn_date = :d"), {"d": d})
        conn.execute(text(_CARD_TXN_INSERT), {"d": d})
        conn.execute(text(_CARD_TXN_DECLINE_INSERT), {"d": d})
        n_txn = conn.execute(
            text("SELECT COUNT(*) FROM CARD_INT.card_txn WHERE txn_date = :d"), {"d": d}
        ).scalar_one()
        n_dec = conn.execute(
            text("SELECT COUNT(*) FROM CARD_INT.card_txn_decline WHERE txn_date = :d"), {"d": d}
        ).scalar_one()
    return {"card_txn": n_txn, "card_txn_decline": n_dec}

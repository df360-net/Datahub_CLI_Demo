from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as date_cls
from datetime import datetime, timezone
from typing import Any, Callable, Literal, Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection

from .. import urn

Status = Literal["PASSED", "FAILED", "WARNING", "ERROR"]


@dataclass(frozen=True)
class CheckSpec:
    code: str  # firm-mandatory: the standardised name; custom: any string
    description: str
    target_schema: str
    target_table: str
    level: Literal["L1", "L2"]
    sql: str  # may reference :d (the business date)
    pass_if: Callable[[Any], bool]
    target_field: Optional[str] = None  # L2 only
    warning_if: Callable[[Any], bool] = field(default=lambda _x: False)
    expected_value_1: Optional[str] = None
    expected_value_2: Optional[str] = None

    @property
    def target_dataset_urn(self) -> str:
        return urn.dataset_urn(self.target_schema, self.target_table)

    @property
    def source_table(self) -> str:
        return f"{self.target_schema}.{self.target_table}"


@dataclass(frozen=True)
class CheckResult:
    spec: CheckSpec
    status: Status
    actual_value: Any
    message: str
    started_at: datetime
    ended_at: datetime
    error: Optional[str] = None


def mandatory_checks(schema: str, table: str, pk: str, min_rows: int, max_rows: int,
                     biz_col: str = "load_business_date") -> list[CheckSpec]:
    """The 5 firm-mandatory dataset-level checks for one table.

    biz_col is the table's business-date column — load_business_date for the
    raw stage tables, txn_date for integration/report.
    """
    fq = f"{schema}.{table}"
    return [
        CheckSpec(
            "SLA_VALIDATION",
            f"Latest loaded_at on {fq} within 24h of now.",
            schema, table, "L1",
            f"SELECT DATEDIFF(SECOND, MAX(loaded_at), SYSUTCDATETIME())/3600.0 "
            f"FROM {fq} WHERE {biz_col} = :d",
            pass_if=lambda h: h is not None and h <= 24.0,
            expected_value_2="24",
        ),
        CheckSpec(
            "RECORD_COUNT",
            f"Row count for {fq} on the business date (informational).",
            schema, table, "L1",
            f"SELECT COUNT(*) FROM {fq} WHERE {biz_col} = :d",
            pass_if=lambda n: n is not None and n >= 0,
        ),
        CheckSpec(
            "BUSINESS_DATE",
            f"{fq} rows all carry a business date in [today-1, today].",
            schema, table, "L1",
            f"SELECT COUNT(*) FROM {fq} WHERE {biz_col} NOT BETWEEN "
            f"CAST(DATEADD(DAY,-1,GETDATE()) AS date) AND CAST(GETDATE() AS date)",
            pass_if=lambda n: n == 0,
        ),
        CheckSpec(
            "RECORD_COUNT_THRESHOLD",
            f"{fq} daily count within [{min_rows}, {max_rows}].",
            schema, table, "L1",
            f"SELECT COUNT(*) FROM {fq} WHERE {biz_col} = :d",
            pass_if=lambda n: n is not None and min_rows <= n <= max_rows,
            expected_value_1=str(min_rows),
            expected_value_2=str(max_rows),
        ),
        CheckSpec(
            "DUPLICATE_FEED",
            f"No duplicate {pk} on {fq} for the business date.",
            schema, table, "L1",
            f"SELECT COALESCE(MAX(c), 0) FROM (SELECT COUNT(*) c FROM {fq} "
            f"WHERE {biz_col} = :d GROUP BY {pk} HAVING COUNT(*) > 1) s",
            pass_if=lambda n: n == 0,
        ),
    ]


def _custom(code, desc, schema, table, sql, pass_if, target_field=None) -> CheckSpec:
    return CheckSpec(
        code, desc, schema, table,
        "L2" if target_field else "L1",
        sql, pass_if, target_field=target_field,
    )


def stage_specs() -> list[CheckSpec]:
    auth = "CARD_STG.card_auth"
    post = "CARD_STG.card_post"
    specs = (
        mandatory_checks("CARD_STG", "card_auth", "txn_id", 100, 1000)
        + mandatory_checks("CARD_STG", "card_post", "posting_ref", 100, 1000)
    )
    specs += [
        _custom("AUTH_AMOUNT_NUMERIC", "Every auth_amount parses as a positive decimal.",
                "CARD_STG", "card_auth",
                f"SELECT COUNT(*) FROM {auth} WHERE load_business_date = :d AND "
                f"(TRY_CAST(auth_amount AS decimal(18,2)) IS NULL OR TRY_CAST(auth_amount AS decimal(18,2)) <= 0)",
                lambda n: n == 0, target_field="auth_amount"),
        _custom("AUTH_TS_PARSEABLE", "Every auth_ts parses as a timestamp.",
                "CARD_STG", "card_auth",
                f"SELECT COUNT(*) FROM {auth} WHERE load_business_date = :d AND "
                f"TRY_CAST(auth_ts AS datetime2(3)) IS NULL",
                lambda n: n == 0, target_field="auth_ts"),
        _custom("MCC_FOUR_DIGITS", "mcc is exactly 4 digits.",
                "CARD_STG", "card_auth",
                f"SELECT COUNT(*) FROM {auth} WHERE load_business_date = :d AND "
                f"mcc NOT LIKE '[0-9][0-9][0-9][0-9]'",
                lambda n: n == 0, target_field="mcc"),
        _custom("TXN_TYPE_ENUM", "txn_type in {PURCHASE, REFUND, AUTH_ONLY}.",
                "CARD_STG", "card_auth",
                f"SELECT COUNT(*) FROM {auth} WHERE load_business_date = :d AND "
                f"txn_type NOT IN ('PURCHASE','REFUND','AUTH_ONLY')",
                lambda n: n == 0, target_field="txn_type"),
        _custom("SETTLED_AMOUNT_NUMERIC", "Every settled_amount parses as a positive decimal.",
                "CARD_STG", "card_post",
                f"SELECT COUNT(*) FROM {post} WHERE load_business_date = :d AND "
                f"(TRY_CAST(settled_amount AS decimal(18,2)) IS NULL OR TRY_CAST(settled_amount AS decimal(18,2)) <= 0)",
                lambda n: n == 0, target_field="settled_amount"),
        _custom("POSTING_TS_PARSEABLE", "Every posting_ts parses as a timestamp.",
                "CARD_STG", "card_post",
                f"SELECT COUNT(*) FROM {post} WHERE load_business_date = :d AND "
                f"TRY_CAST(posting_ts AS datetime2(3)) IS NULL",
                lambda n: n == 0, target_field="posting_ts"),
        _custom("POST_TXN_ID_IN_AUTH", "Every posting txn_id exists in the same day's auth feed.",
                "CARD_STG", "card_post",
                f"SELECT COUNT(*) FROM {post} p WHERE p.load_business_date = :d AND NOT EXISTS "
                f"(SELECT 1 FROM {auth} a WHERE a.txn_id = p.txn_id "
                f"AND a.load_business_date = p.load_business_date)",
                lambda n: n == 0, target_field="txn_id"),
    ]
    return specs


def integration_specs() -> list[CheckSpec]:
    ct = "CARD_INT.card_txn"
    dec = "CARD_INT.card_txn_decline"
    specs = (
        mandatory_checks("CARD_INT", "card_txn", "txn_id", 100, 1000, biz_col="txn_date")
        # card_txn_decline is the ~5% decline subset, so its volume band is far
        # below the design's lumped CARD_INT 100-1000 figure — see note to Jianmin.
        + mandatory_checks("CARD_INT", "card_txn_decline", "txn_id", 1, 100, biz_col="txn_date")
    )
    specs += [
        _custom("CARD_ID_RESOLVED", "Every card_txn row resolves a card_id.",
                "CARD_INT", "card_txn",
                f"SELECT COUNT(*) FROM {ct} WHERE txn_date = :d AND card_id IS NULL",
                lambda n: n == 0, target_field="card_id"),
        _custom("MERCHANT_NAME_PRESENT", "Every card_txn row has a merchant_name.",
                "CARD_INT", "card_txn",
                f"SELECT COUNT(*) FROM {ct} WHERE txn_date = :d AND merchant_name IS NULL",
                lambda n: n == 0, target_field="merchant_name"),
        _custom("AUTH_AMOUNT_RANGE", "card_txn.auth_amount in (0, 100000].",
                "CARD_INT", "card_txn",
                f"SELECT COUNT(*) FROM {ct} WHERE txn_date = :d "
                f"AND NOT (auth_amount > 0 AND auth_amount <= 100000)",
                lambda n: n == 0, target_field="auth_amount"),
        _custom("APPROVED_FLAG_CONSISTENT", "is_approved agrees with decline_reason presence.",
                "CARD_INT", "card_txn",
                f"SELECT COUNT(*) FROM {ct} WHERE txn_date = :d AND "
                f"((is_approved = 1 AND decline_reason IS NOT NULL) OR "
                f"(is_approved = 0 AND decline_reason IS NULL))",
                lambda n: n == 0),
        _custom("POSTED_IMPLIES_APPROVED", "is_posted = 1 only when is_approved = 1.",
                "CARD_INT", "card_txn",
                f"SELECT COUNT(*) FROM {ct} WHERE txn_date = :d AND is_posted = 1 AND is_approved = 0",
                lambda n: n == 0),
        _custom("DECLINE_REASON_PRESENT", "Every card_txn_decline row has a decline_reason.",
                "CARD_INT", "card_txn_decline",
                f"SELECT COUNT(*) FROM {dec} WHERE txn_date = :d AND decline_reason IS NULL",
                lambda n: n == 0, target_field="decline_reason"),
    ]
    return specs


def report_specs() -> list[CheckSpec]:
    summ = "CARD_RPT.daily_card_summary"
    topm = "CARD_RPT.card_top_merchants"
    specs = (
        # Composite-PK group for DUPLICATE_FEED; the table PK makes dups
        # physically impossible, so this is informational here.
        mandatory_checks("CARD_RPT", "daily_card_summary", "mcc_category, txn_type",
                         1, 200, biz_col="txn_date")
        + mandatory_checks("CARD_RPT", "card_top_merchants", "[rank]",
                           1, 10, biz_col="txn_date")
    )
    specs += [
        _custom("APPROVED_PLUS_DECLINE_EQUALS_TXN_COUNT",
                "Per summary row: approved_count + decline_count == txn_count.",
                "CARD_RPT", "daily_card_summary",
                f"SELECT COUNT(*) FROM {summ} WHERE txn_date = :d "
                f"AND approved_count + decline_count <> txn_count",
                lambda n: n == 0),
        _custom("RANK_IS_1_TO_10", "card_top_merchants rank within 1..10.",
                "CARD_RPT", "card_top_merchants",
                f"SELECT COUNT(*) FROM {topm} WHERE txn_date = :d "
                f"AND ([rank] < 1 OR [rank] > 10)",
                lambda n: n == 0, target_field="rank"),
    ]
    return specs


def _run_single(conn: Connection, spec: CheckSpec, biz_date: date_cls) -> CheckResult:
    started = datetime.now(timezone.utc)
    try:
        row = conn.execute(text(spec.sql), {"d": str(biz_date)}).fetchone()
    except Exception as exc:  # noqa: BLE001 — any SQL failure becomes an ERROR result
        return CheckResult(spec, "ERROR", None, f"SQL error: {exc}",
                           started, datetime.now(timezone.utc), str(exc))

    actual = row[0] if row is not None else None
    try:
        actual_num: Any = float(actual) if actual is not None else None
    except (TypeError, ValueError):
        actual_num = actual

    ended = datetime.now(timezone.utc)
    if spec.warning_if(actual_num):
        return CheckResult(spec, "WARNING", actual_num,
                           f"WARNING: {spec.description} (actual={actual_num})", started, ended)
    if spec.pass_if(actual_num):
        return CheckResult(spec, "PASSED", actual_num,
                           f"OK: {spec.description}", started, ended)
    return CheckResult(spec, "FAILED", actual_num,
                       f"FAIL: {spec.description} (actual={actual_num})", started, ended)


def run_checks(conn: Connection, specs: list[CheckSpec], biz_date: date_cls) -> list[CheckResult]:
    return [_run_single(conn, s, biz_date) for s in specs]

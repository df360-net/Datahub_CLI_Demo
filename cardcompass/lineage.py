from __future__ import annotations

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

from . import urn
from .emit import emit_aspect


def _ds(schema: str, table: str) -> str:
    return urn.dataset_urn(schema, table)


# downstream (schema, table) -> upstream dataset URNs (table-level edges).
# Note: card_txn_decline is built FROM card_txn only (the decline subset), so
# its single upstream is card_txn — card_auth is reached transitively. This is
# accurate to the SQL; it differs slightly from the design diagram which also
# draws a direct card_auth edge.
TABLE_UPSTREAMS = {
    ("CARD_STG", "card_auth"): [urn.file_urn("card_auth_inbound")],
    ("CARD_STG", "card_post"): [urn.file_urn("card_post_inbound")],
    ("CARD_INT", "card_txn"): [
        _ds("CARD_STG", "card_auth"), _ds("CARD_STG", "card_post"),
        _ds("CARD_REF", "card"), _ds("CARD_REF", "merchant"), _ds("CARD_REF", "mcc_category"),
    ],
    ("CARD_INT", "card_txn_decline"): [_ds("CARD_INT", "card_txn")],
    ("CARD_RPT", "daily_card_summary"): [_ds("CARD_INT", "card_txn")],
    ("CARD_RPT", "card_top_merchants"): [_ds("CARD_INT", "card_txn")],
}

# downstream (schema, table) -> [(downstream_col, [(up_schema, up_table, up_col), ...])]
COLUMN_EDGES = {
    ("CARD_INT", "card_txn"): [
        ("txn_id", [("CARD_STG", "card_auth", "txn_id")]),
        ("card_id", [("CARD_STG", "card_auth", "card_number_hash"),
                     ("CARD_REF", "card", "card_number_hash"), ("CARD_REF", "card", "card_id")]),
        ("auth_ts", [("CARD_STG", "card_auth", "auth_ts")]),
        ("posting_ts", [("CARD_STG", "card_post", "posting_ts")]),
        ("auth_amount", [("CARD_STG", "card_auth", "auth_amount")]),
        ("settled_amount", [("CARD_STG", "card_post", "settled_amount")]),
        ("mcc_category", [("CARD_STG", "card_auth", "mcc"), ("CARD_REF", "mcc_category", "mcc_category")]),
        ("merchant_name", [("CARD_STG", "card_auth", "merchant_id"), ("CARD_REF", "merchant", "merchant_name")]),
        ("is_approved", [("CARD_STG", "card_auth", "response_code")]),
        ("is_posted", [("CARD_STG", "card_post", "txn_id")]),
        ("decline_reason", [("CARD_STG", "card_auth", "response_code")]),
    ],
    ("CARD_INT", "card_txn_decline"): [
        ("txn_id", [("CARD_INT", "card_txn", "txn_id")]),
        ("card_id", [("CARD_INT", "card_txn", "card_id")]),
        ("txn_date", [("CARD_INT", "card_txn", "txn_date")]),
        ("auth_amount", [("CARD_INT", "card_txn", "auth_amount")]),
        ("decline_reason", [("CARD_INT", "card_txn", "decline_reason")]),
        ("flagged_for_review", [("CARD_INT", "card_txn", "auth_amount")]),
    ],
    ("CARD_RPT", "daily_card_summary"): [
        ("txn_date", [("CARD_INT", "card_txn", "txn_date")]),
        ("mcc_category", [("CARD_INT", "card_txn", "mcc_category")]),
        ("txn_type", [("CARD_INT", "card_txn", "txn_type")]),
        ("txn_count", [("CARD_INT", "card_txn", "txn_id")]),
        ("approved_count", [("CARD_INT", "card_txn", "is_approved")]),
        ("decline_count", [("CARD_INT", "card_txn", "is_approved")]),
        ("auth_amount_total", [("CARD_INT", "card_txn", "auth_amount"), ("CARD_INT", "card_txn", "is_approved")]),
        ("settled_amount_total", [("CARD_INT", "card_txn", "settled_amount"), ("CARD_INT", "card_txn", "is_approved")]),
    ],
    ("CARD_RPT", "card_top_merchants"): [
        ("merchant_id", [("CARD_INT", "card_txn", "merchant_id")]),
        ("merchant_name", [("CARD_INT", "card_txn", "merchant_name")]),
        ("rank", [("CARD_INT", "card_txn", "settled_amount")]),
        ("settled_amount_total", [("CARD_INT", "card_txn", "settled_amount")]),
    ],
}


def _fine_grained(down_schema: str, down_table: str) -> list[FineGrainedLineageClass]:
    edges = COLUMN_EDGES.get((down_schema, down_table), [])
    fgl = []
    for down_col, ups in edges:
        fgl.append(FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[urn.column_urn(s, t, c) for s, t, c in ups],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            downstreams=[urn.column_urn(down_schema, down_table, down_col)],
            confidenceScore=1.0,
        ))
    return fgl


def publish_lineage(emitter: DatahubRestEmitter) -> int:
    """Publish upstreamLineage (table edges + fineGrainedLineages) per
    downstream dataset. Fail-fast on any emit failure."""
    published = 0
    for (schema, table), upstreams in TABLE_UPSTREAMS.items():
        aspect = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=u, type=DatasetLineageTypeClass.TRANSFORMED)
                for u in upstreams
            ],
            fineGrainedLineages=_fine_grained(schema, table) or None,
        )
        emit_aspect(emitter, urn.dataset_urn(schema, table), aspect)
        published += 1
    return published

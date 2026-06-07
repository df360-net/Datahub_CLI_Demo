from __future__ import annotations

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TimeTypeClass,
)

from . import urn
from .emit import emit_aspect

_PLATFORM_URN = "urn:li:dataPlatform:mssql"

# (schema, table, description, [(column, mssql_native_type)])
DATASETS = [
    ("CARD_STG", "card_auth", "Raw landing of the daily authorization feed.", [
        ("txn_id", "nvarchar(40)"), ("card_number_hash", "nvarchar(64)"),
        ("auth_ts", "nvarchar(32)"), ("auth_amount", "nvarchar(20)"),
        ("txn_currency", "nvarchar(3)"), ("merchant_id", "nvarchar(20)"),
        ("mcc", "nvarchar(4)"), ("txn_type", "nvarchar(20)"),
        ("response_code", "nvarchar(4)"), ("file_seq", "nvarchar(20)"),
        ("load_business_date", "date"), ("loaded_at", "datetime2(3)"),
    ]),
    ("CARD_STG", "card_post", "Raw landing of the daily settlement/posting feed.", [
        ("txn_id", "nvarchar(40)"), ("posting_ref", "nvarchar(40)"),
        ("posting_ts", "nvarchar(32)"), ("settled_amount", "nvarchar(20)"),
        ("settlement_currency", "nvarchar(3)"), ("acquirer_id", "nvarchar(20)"),
        ("file_seq", "nvarchar(20)"), ("load_business_date", "date"),
        ("loaded_at", "datetime2(3)"),
    ]),
    ("CARD_INT", "card_txn", "One row per real card transaction (auth joined to posting, enriched).", [
        ("txn_id", "nvarchar(40)"), ("card_id", "bigint"),
        ("auth_ts", "datetime2(3)"), ("posting_ts", "datetime2(3)"),
        ("txn_date", "date"), ("auth_amount", "decimal(18,2)"),
        ("settled_amount", "decimal(18,2)"), ("txn_currency", "char(3)"),
        ("merchant_id", "nvarchar(20)"), ("merchant_name", "nvarchar(100)"),
        ("mcc", "char(4)"), ("mcc_category", "nvarchar(50)"),
        ("txn_type", "nvarchar(20)"), ("is_approved", "bit"),
        ("is_posted", "bit"), ("decline_reason", "nvarchar(100)"),
        ("loaded_at", "datetime2(3)"),
    ]),
    ("CARD_INT", "card_txn_decline", "Declined transactions split out for risk/fraud consumers.", [
        ("txn_id", "nvarchar(40)"), ("card_id", "bigint"),
        ("txn_date", "date"), ("auth_amount", "decimal(18,2)"),
        ("decline_reason", "nvarchar(100)"), ("flagged_for_review", "bit"),
        ("loaded_at", "datetime2(3)"),
    ]),
    ("CARD_RPT", "daily_card_summary", "Daily totals per (txn_date, mcc_category, txn_type).", [
        ("txn_date", "date"), ("mcc_category", "nvarchar(50)"),
        ("txn_type", "nvarchar(20)"), ("txn_count", "int"),
        ("approved_count", "int"), ("decline_count", "int"),
        ("auth_amount_total", "decimal(18,2)"), ("settled_amount_total", "decimal(18,2)"),
        ("loaded_at", "datetime2(3)"),
    ]),
    ("CARD_RPT", "card_top_merchants", "Top 10 merchants per day by approved settled volume.", [
        ("txn_date", "date"), ("rank", "int"), ("merchant_id", "nvarchar(20)"),
        ("merchant_name", "nvarchar(100)"), ("txn_count", "int"),
        ("settled_amount_total", "decimal(18,2)"), ("loaded_at", "datetime2(3)"),
    ]),
]


def _field_type(native: str) -> SchemaFieldDataTypeClass:
    n = native.lower()
    if n.startswith(("nvarchar", "varchar", "char")):
        cls = StringTypeClass()
    elif n.startswith(("decimal", "numeric", "bigint", "int", "float", "real", "smallint")):
        cls = NumberTypeClass()
    elif n.startswith("bit"):
        cls = BooleanTypeClass()
    elif n.startswith("datetime"):
        cls = TimeTypeClass()
    elif n.startswith("date"):
        cls = DateTypeClass()
    else:
        cls = StringTypeClass()
    return SchemaFieldDataTypeClass(type=cls)


def _schema_aspect(schema: str, table: str, cols) -> SchemaMetadataClass:
    fields = [
        SchemaFieldClass(
            fieldPath=name,
            type=_field_type(native),
            nativeDataType=native,
        )
        for name, native in cols
    ]
    return SchemaMetadataClass(
        schemaName=f"{schema}.{table}",
        platform=_PLATFORM_URN,
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=fields,
    )


def publish_catalog(emitter: DatahubRestEmitter, source_app: str) -> int:
    """Publish dataset properties + schema metadata for all 6 datasets.
    Fail-fast: any emit failure raises DataHubEmitError (caught upstream)."""
    published = 0
    for schema, table, description, cols in DATASETS:
        ds_urn = urn.dataset_urn(schema, table)
        emit_aspect(emitter, ds_urn, DatasetPropertiesClass(
            name=table,
            description=description,
            customProperties={
                "source.app": source_app,
                "source.table": f"{schema}.{table}",
            },
        ))
        emit_aspect(emitter, ds_urn, _schema_aspect(schema, table, cols))
        published += 1
    return published

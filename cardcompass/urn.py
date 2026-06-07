from __future__ import annotations

import os

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from dotenv import load_dotenv

load_dotenv()

PLATFORM = "mssql"
DATABASE = "DCF_DB"
ENV = "PROD"


def enterprise_pid() -> str:
    """The firm Enterprise PID — namespaces every URN as platformInstance."""
    pid = os.getenv("ENTERPRISE_PID")
    if not pid:
        raise SystemExit("ENTERPRISE_PID is required (set it in .env) — every URN is namespaced by it")
    return pid


def dataset_name(schema: str, table: str, database: str = DATABASE) -> str:
    return f"{database}.{schema}.{table}"


def dataset_urn(schema: str, table: str, database: str = DATABASE) -> str:
    return make_dataset_urn_with_platform_instance(
        PLATFORM, dataset_name(schema, table, database), enterprise_pid(), ENV
    )


def column_urn(schema: str, table: str, column: str, database: str = DATABASE) -> str:
    return make_schema_field_urn(dataset_urn(schema, table, database), column)


def file_urn(name: str) -> str:
    """Stable logical 'inbound feed' dataset on the file platform — the
    lineage origin for a stage table (not a per-date file)."""
    return make_dataset_urn_with_platform_instance("file", name, enterprise_pid(), ENV)

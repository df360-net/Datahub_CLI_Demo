"""URN builders. Datasets carry NO platformInstance - the single, clickable SAL360 Application
entity is the cross-platform home instead. Each dataset still nests under a per-platform schema
Container (sales_bronze, etc.)."""
from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_data_platform_urn,
    make_dataset_urn,
    make_schema_field_urn,
)

ORCHESTRATOR = "airflow"
FLOW_ID = "sales_daily"


def dataset_urn(platform: str, name: str, env: str = "PROD") -> str:
    return make_dataset_urn(platform, name, env)


def app_urn(app_id: str) -> str:
    return f"urn:li:application:{app_id}"


def platform_urn(platform: str) -> str:
    return make_data_platform_urn(platform)


def flow_urn(cluster: str = "PROD") -> str:
    return make_data_flow_urn(ORCHESTRATOR, FLOW_ID, cluster)


def job_urn(job_id: str, cluster: str = "PROD") -> str:
    return make_data_job_urn(ORCHESTRATOR, FLOW_ID, job_id, cluster)


def field_urn(dataset_urn_: str, column: str) -> str:
    return make_schema_field_urn(dataset_urn_, column)

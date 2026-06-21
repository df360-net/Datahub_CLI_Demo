"""URN builders. Every dataset is namespaced by the firm PID (DataHub platformInstance) so the
graph is unique firm-wide - the same convention CardCompass uses, kept separate from it."""
from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)

ORCHESTRATOR = "airflow"
FLOW_ID = "sales_daily"


def dataset_urn(platform: str, name: str, pid: str, env: str = "PROD") -> str:
    return make_dataset_urn_with_platform_instance(platform, name, pid, env)


def platform_urn(platform: str) -> str:
    return make_data_platform_urn(platform)


def flow_urn(cluster: str = "PROD") -> str:
    return make_data_flow_urn(ORCHESTRATOR, FLOW_ID, cluster)


def job_urn(job_id: str, cluster: str = "PROD") -> str:
    return make_data_job_urn(ORCHESTRATOR, FLOW_ID, job_id, cluster)


def field_urn(dataset_urn_: str, column: str) -> str:
    return make_schema_field_urn(dataset_urn_, column)

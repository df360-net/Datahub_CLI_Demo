"""URN builders. Everything lives under one logical platform (`sales360`) and one
platformInstance (ENTERPRISE_APP_ID, e.g. sales360_demo) - so the instance is the single
clickable top container and rides in every dataset URN. Airflow entities carry the instance in
their dataFlow/dataJob `cluster`."""
from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)

ORCHESTRATOR = "airflow"
FLOW_ID = "sales_daily"


def dataset_urn(platform: str, name: str, instance: str, env: str = "PROD") -> str:
    return make_dataset_urn_with_platform_instance(platform, name, instance, env)


def platform_urn(platform: str) -> str:
    return make_data_platform_urn(platform)


def instance_urn(platform: str, instance: str) -> str:
    return make_dataplatform_instance_urn(platform, instance)


def flow_urn(cluster: str) -> str:
    return make_data_flow_urn(ORCHESTRATOR, FLOW_ID, cluster)


def job_urn(job_id: str, cluster: str) -> str:
    return make_data_job_urn(ORCHESTRATOR, FLOW_ID, job_id, cluster)


def field_urn(dataset_urn_: str, column: str) -> str:
    return make_schema_field_urn(dataset_urn_, column)

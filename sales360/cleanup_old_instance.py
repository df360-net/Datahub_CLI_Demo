"""One-shot: soft-delete the OLD platformInstance=SAL360 datasets and the 4 dataPlatformInstance
grouping nodes, after re-publishing under the new (instance-free) URNs. Soft delete removes them
from browse/search (reversible). Run once:  python -m sales360.cleanup_old_instance
"""
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

from sales360.config import Settings
from sales360.model import LAYER_PLATFORM, TABLES

OLD_PID = "SAL360"


def main():
    s = Settings.load()
    g = DataHubGraph(DatahubClientConfig(server=s.gms_server, token=s.token))

    urns = []
    for layer, tables in TABLES.items():
        platform, ns = LAYER_PLATFORM[layer]
        for table in tables:
            urns.append(make_dataset_urn_with_platform_instance(platform, f"{ns}.{table}", OLD_PID, s.env))
    for platform in {LAYER_PLATFORM[layer][0] for layer in LAYER_PLATFORM}:
        urns.append(f"urn:li:dataPlatformInstance:({make_data_platform_urn(platform)},{OLD_PID})")

    n = 0
    for u in urns:
        try:
            g.soft_delete_entity(u)
            n += 1
        except Exception as e:
            print(f"  skip {u}: {e}")
    print(f"soft-deleted {n}/{len(urns)} old platformInstance entities")


if __name__ == "__main__":
    main()

"""Fetch the full entity JSON (all aspects) for a DataHub URN.

Uses the /entitiesV2 endpoint, which returns every aspect and works where the legacy
/entities snapshot endpoint errors.

Usage:
    python tools/get_entity.py "<urn>"

Env (repo-root .env or shell):
    DATAHUB_ACCESS_TOKEN   required
    DATAHUB_GMS_URL        default http://192.168.0.16:8080
"""
import json
import os
import sys

from dotenv import load_dotenv
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig


def _graph() -> DataHubGraph:
    here = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.normpath(os.path.join(here, "..", ".env")))
    token = os.environ.get("DATAHUB_ACCESS_TOKEN")
    if not token:
        raise SystemExit("DATAHUB_ACCESS_TOKEN not set in .env")
    server = os.environ.get("DATAHUB_GMS_URL", "http://192.168.0.16:8080")
    return DataHubGraph(DataHubGraphConfig(server=server, token=token))


def get_entity(urn: str, graph: DataHubGraph = None) -> dict:
    return (graph or _graph()).get_entity_raw(urn)


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print('usage: python tools/get_entity.py "<urn>"', file=sys.stderr)
        return 2
    print(json.dumps(get_entity(argv[0]), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

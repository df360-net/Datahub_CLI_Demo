"""Fetch the full entity JSON for the URN embedded in a DataHub UI link.

Composes get_urn_from_link + get_entity: pull the URN out of the link, then fetch all aspects.

Usage:
    python tools/get_entity_from_link.py "<datahub-url>"
"""
import json
import sys

try:
    from get_urn_from_link import urn_from_link
    from get_entity import get_entity
except ImportError:
    from tools.get_urn_from_link import urn_from_link
    from tools.get_entity import get_entity


def get_entity_from_link(link: str) -> dict:
    urn = urn_from_link(link)
    if not urn:
        raise SystemExit(f"no URN found in link: {link}")
    return get_entity(urn)


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print('usage: python tools/get_entity_from_link.py "<datahub-url>"', file=sys.stderr)
        return 2
    print(json.dumps(get_entity_from_link(argv[0]), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

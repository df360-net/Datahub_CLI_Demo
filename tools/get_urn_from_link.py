"""Extract DataHub URN(s) from a DataHub UI link.

Two link shapes are handled:
  - entity pages   -> the URN sits raw in the path: /dataset/urn:li:dataset:(...)/Columns
  - search pages   -> URN-valued filter params:    /search?filter_platform___..=urn%3Ali%3A..

Non-URN filters (e.g. a browsePathV2 value like the unit-separator-prefixed sales360_demo)
are ignored. URNs are returned in the order found, de-duplicated.

Usage:
    python tools/get_urn_from_link.py "<datahub-url>" ["<url2>" ...]

As a module:
    from tools.get_urn_from_link import urns_from_link, urn_from_link
"""
import re
import sys
from urllib.parse import parse_qsl, unquote, urlparse

# A URN runs until the next path/query delimiter; nested urn:li: parts are swallowed as content.
_URN_RE = re.compile(r"urn:li:[^\s/?#&]+")


def urns_from_link(link: str) -> list[str]:
    parsed = urlparse(link.strip())

    found = _URN_RE.findall(unquote(parsed.path))
    for _, value in parse_qsl(parsed.query, keep_blank_values=True):
        if value.startswith("urn:li:"):
            found.append(value)

    seen: set[str] = set()
    return [u for u in found if not (u in seen or seen.add(u))]


def urn_from_link(link: str) -> str | None:
    urns = urns_from_link(link)
    return urns[0] if urns else None


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__.strip().splitlines()[0], file=sys.stderr)
        print('usage: python tools/get_urn_from_link.py "<datahub-url>"', file=sys.stderr)
        return 2
    rc = 1
    for link in argv:
        for urn in urns_from_link(link):
            print(urn)
            rc = 0
    return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

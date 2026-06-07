from __future__ import annotations

import requests

from .config import ProxyConfig
from .upstream import Upstream


def build_health(cfg: ProxyConfig, upstream: Upstream) -> tuple[int, dict]:
    try:
        resp = upstream.ping()
    except requests.RequestException as exc:
        return 503, {
            "status": "degraded",
            "upstream": "unreachable",
            "upstream_url": cfg.datahub_url,
            "last_error": f"GET /config failed: {type(exc).__name__}",
        }

    reachable = resp.status_code == 200
    body = {
        "status": "ok" if reachable else "degraded",
        "upstream": "reachable" if reachable else "unreachable",
        "upstream_url": cfg.datahub_url,
        "last_ping_status": resp.status_code,
    }
    if upstream.auth is not None:
        body.update(upstream.auth.health())
    return (200 if reachable else 503), body

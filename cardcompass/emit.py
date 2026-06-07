from __future__ import annotations

from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

from .config import AppConfig


class DataHubEmitError(Exception):
    """Raised when an SDK emit through the proxy fails (bad status or transport)."""


def make_emitter(cfg: AppConfig) -> DatahubRestEmitter:
    # /api/gms: the frontend mounts the Restli API there (the SDK's default
    # endpoint). token="" -> no Authorization header; the proxy injects the
    # session cookies.
    return DatahubRestEmitter(gms_server=f"{cfg.proxy_url}/api/gms", token="")


def emit_aspect(emitter: DatahubRestEmitter, entity_urn: str, aspect) -> None:
    """Emit one aspect, capturing the outcome. Never dump-and-forget:
    emit() raises OperationalError on non-2xx and wraps transport errors; we
    surface either as DataHubEmitError with the URN for context."""
    try:
        emitter.emit(MetadataChangeProposalWrapper(entityUrn=entity_urn, aspect=aspect))
    except OperationalError as exc:
        raise DataHubEmitError(f"emit failed for {entity_urn}: {exc}") from exc
    except Exception as exc:  # noqa: BLE001 — any failure must be surfaced, not swallowed
        raise DataHubEmitError(
            f"emit error for {entity_urn}: {type(exc).__name__}: {exc}"
        ) from exc

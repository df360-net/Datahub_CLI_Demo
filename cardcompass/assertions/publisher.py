from __future__ import annotations

from dataclasses import dataclass

import requests

from .specs import CheckResult

# DataHub's run-event result.type enum is SUCCESS/FAILURE/ERROR only; local
# WARNING collapses to FAILURE, with the precise status kept in
# nativeResults.detail_status so consumers can recover it.
RUN_RESULT_TYPE = {
    "PASSED": "SUCCESS",
    "FAILED": "FAILURE",
    "WARNING": "FAILURE",
    "ERROR": "ERROR",
}


class DataHubPublishError(Exception):
    """Raised when DataHub (via the proxy) rejects an assertion POST."""


class DataHubTransportError(DataHubPublishError):
    """Transport-level failure (proxy unreachable, timeout). Means every
    remaining POST will fail too, so the batch aborts immediately."""


@dataclass(frozen=True)
class PublishSummary:
    posted: int
    failed: int
    errors: list[str]


def _assertion_urn(pid: str, result: CheckResult) -> str:
    spec = result.spec
    # Unique per (PID, dataset, check) — the same code targets multiple
    # datasets, so the dataset identity must be in the URN.
    raw = f"{pid}_{spec.target_schema}_{spec.target_table}_{spec.code}"
    return f"urn:li:assertion:{raw.lower()}"


def _build_entity(result: CheckResult, source_app: str, pid: str) -> dict:
    spec = result.spec
    ts_ms = int(result.ended_at.timestamp() * 1000)
    assertion_urn = _assertion_urn(pid, result)

    custom_props = {
        "source.app": source_app,
        "source.table": spec.source_table,
        "source.check_code": spec.code,
    }
    if spec.target_field:
        custom_props["source.field"] = spec.target_field

    # The assertion DEFINITION must be byte-stable run to run, so re-publishing
    # it daily is a DataHub no-op (no new version, no MCL/CDC event). It changes
    # only when the check itself changes. No run-time timestamp here — the daily
    # pass/fail history lives entirely in the timeseries assertionRunEvent below.
    assertion_info = {
        "type": "CUSTOM",
        "customAssertion": {
            "entity": spec.target_dataset_urn,  # DataHub requires a dataset URN here
            "type": spec.code,                  # the standardised / free-form name
            "logic": spec.sql,
        },
        "description": spec.description,
        "customProperties": custom_props,
    }

    native: dict = {
        "actual_value": str(result.actual_value),
        "detail_status": result.status,
        "message": result.message,
    }
    if spec.expected_value_1 is not None:
        native["expected_value_1"] = spec.expected_value_1
    if spec.expected_value_2 is not None:
        native["expected_value_2"] = spec.expected_value_2
    if result.error:
        native["error"] = result.error

    run_event = {
        "__type": "AssertionRunEvent",
        "timestampMillis": ts_ms,
        "runId": f"{spec.code.lower()}-{ts_ms}",
        "assertionUrn": assertion_urn,
        "asserteeUrn": spec.target_dataset_urn,
        "status": "COMPLETE",
        "partitionSpec": {"type": "FULL_TABLE", "partition": "FULL_TABLE_SNAPSHOT"},
        "result": {"type": RUN_RESULT_TYPE[result.status], "nativeResults": native},
    }

    return {
        "urn": assertion_urn,
        "assertionInfo": {"value": assertion_info},
        "assertionRunEvent": {"value": run_event},
    }


def _post(proxy_url: str, entity: dict) -> None:
    url = f"{proxy_url}/openapi/v3/entity/assertion?async=false&systemMetadata=false"
    try:
        resp = requests.post(url, json=[entity], timeout=30.0)
    except requests.RequestException as exc:
        # transport failure (proxy down, timeout, DNS): capture, never swallow
        raise DataHubTransportError(f"transport error: {type(exc).__name__}: {exc}") from exc
    if resp.status_code >= 300:
        raise DataHubPublishError(f"publish failed: {resp.status_code} {resp.text[:200]}")


def publish_results(proxy_url: str, results: list[CheckResult],
                    source_app: str, pid: str) -> PublishSummary:
    """Publish each CheckResult as one assertion entity + run event, via the
    proxy. Single-entity POSTs so one bad result doesn't drop the batch."""
    posted = 0
    failed = 0
    errors: list[str] = []
    for r in results:
        try:
            _post(proxy_url, _build_entity(r, source_app, pid))
            posted += 1
        except DataHubTransportError as exc:
            # proxy unreachable: the rest will fail identically — abort now
            errors.append(f"{r.spec.source_table} {r.spec.code}: {exc}")
            failed += 1
            break
        except DataHubPublishError as exc:
            failed += 1
            errors.append(f"{r.spec.source_table} {r.spec.code}: {exc}")
    return PublishSummary(posted=posted, failed=failed, errors=errors)

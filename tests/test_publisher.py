from datetime import datetime, timezone

import requests
import responses

from cardcompass.assertions.publisher import _build_entity, publish_results
from cardcompass.assertions.specs import CheckResult, CheckSpec

_TS = datetime(2026, 6, 7, tzinfo=timezone.utc)


def _result(status="PASSED", actual=5):
    spec = CheckSpec(
        "RECORD_COUNT", "Row count.", "CARD_STG", "card_auth", "L1",
        "SELECT 1", lambda n: True, expected_value_1="1", expected_value_2="10",
    )
    return CheckResult(spec, status, actual, "msg", _TS, _TS)


def test_assertion_urn_is_pid_and_dataset_scoped():
    ent = _build_entity(_result(), "CardCompass", "TEST")
    assert ent["urn"] == "urn:li:assertion:test_card_stg_card_auth_record_count"


def test_build_entity_shape():
    info = _build_entity(_result(), "CardCompass", "TEST")["assertionInfo"]["value"]
    assert info["customAssertion"]["type"] == "RECORD_COUNT"
    assert info["customAssertion"]["entity"].startswith("urn:li:dataset:")
    assert info["customProperties"]["source.app"] == "CardCompass"
    assert info["customProperties"]["source.table"] == "CARD_STG.card_auth"


def test_custom_properties_are_generic_no_df360():
    info = _build_entity(_result(), "CardCompass", "TEST")["assertionInfo"]["value"]
    props = info["customProperties"]
    assert all(k.startswith("source.") for k in props)        # only generic source.* keys
    assert not any("df360" in k.lower() for k in props)       # producer is consumer-unaware


def test_assertion_definition_is_stable_across_runs():
    # Same check on two different days: the DEFINITION must be byte-identical
    # (so DataHub no-ops it), while the run EVENT differs (timeseries).
    spec = CheckSpec("RECORD_COUNT", "Row count.", "CARD_STG", "card_auth", "L1",
                     "SELECT 1", lambda n: True)
    day1 = CheckResult(spec, "PASSED", 100, "msg", datetime(2026, 6, 7, tzinfo=timezone.utc),
                       datetime(2026, 6, 7, tzinfo=timezone.utc))
    day2 = CheckResult(spec, "PASSED", 105, "msg", datetime(2026, 6, 8, tzinfo=timezone.utc),
                       datetime(2026, 6, 8, tzinfo=timezone.utc))
    e1 = _build_entity(day1, "CardCompass", "TEST")
    e2 = _build_entity(day2, "CardCompass", "TEST")
    assert e1["assertionInfo"] == e2["assertionInfo"]          # definition unchanged
    assert "lastUpdated" not in e1["assertionInfo"]["value"]   # no run-time stamp
    assert e1["assertionRunEvent"] != e2["assertionRunEvent"]  # results move daily


def test_passed_and_failed_map_to_result_type():
    ok = _build_entity(_result("PASSED"), "CardCompass", "TEST")["assertionRunEvent"]["value"]
    bad = _build_entity(_result("FAILED", 2), "CardCompass", "TEST")["assertionRunEvent"]["value"]
    assert ok["result"]["type"] == "SUCCESS"
    assert bad["result"]["type"] == "FAILURE"
    # WARNING/ERROR distinctions preserved out-of-band
    assert bad["result"]["nativeResults"]["detail_status"] == "FAILED"
    assert ok["result"]["nativeResults"]["expected_value_1"] == "1"


@responses.activate
def test_transport_error_aborts_batch_after_first():
    responses.add(
        responses.POST,
        "http://proxy.local/openapi/v3/entity/assertion",
        body=requests.exceptions.ConnectionError("refused"),
    )
    summary = publish_results("http://proxy.local", [_result(), _result(), _result()],
                              "CardCompass", "TEST")
    assert summary.posted == 0
    assert summary.failed == 1          # recorded one error
    assert len(responses.calls) == 1    # did NOT keep trying the rest


@responses.activate
def test_bad_status_is_captured():
    responses.add(responses.POST, "http://proxy.local/openapi/v3/entity/assertion",
                  status=500, body="boom")
    summary = publish_results("http://proxy.local", [_result()], "CardCompass", "TEST")
    assert summary.posted == 0 and summary.failed == 1
    assert "500" in summary.errors[0]

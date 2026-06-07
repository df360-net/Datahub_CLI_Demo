from datetime import date

from cardcompass.assertions import specs

_D = date(2026, 6, 7)


class _Result:
    def __init__(self, value):
        self._value = value

    def fetchone(self):
        return None if self._value is None else (self._value,)


class _Conn:
    """Minimal stand-in for a SQLAlchemy Connection: returns one value, or
    raises, from execute(...).fetchone()."""

    def __init__(self, value=None, raise_exc=None):
        self._value = value
        self._raise = raise_exc

    def execute(self, *a, **k):
        if self._raise is not None:
            raise self._raise
        return _Result(self._value)


def _spec(pass_if, warning_if=None):
    kw = {"warning_if": warning_if} if warning_if else {}
    return specs.CheckSpec("X", "desc", "CARD_STG", "card_auth", "L1", "SELECT 1", pass_if, **kw)


def test_passed():
    r = specs._run_single(_Conn(5), _spec(lambda n: n > 0), _D)
    assert r.status == "PASSED" and r.actual_value == 5.0


def test_failed():
    r = specs._run_single(_Conn(0), _spec(lambda n: n > 0), _D)
    assert r.status == "FAILED"


def test_warning_takes_precedence_over_pass():
    r = specs._run_single(_Conn(5), _spec(lambda n: True, warning_if=lambda n: n == 5), _D)
    assert r.status == "WARNING"


def test_sql_error_becomes_error_result():
    r = specs._run_single(_Conn(raise_exc=RuntimeError("boom")), _spec(lambda n: True), _D)
    assert r.status == "ERROR" and r.error and "boom" in r.error


def test_non_numeric_actual_falls_back_to_raw_value():
    r = specs._run_single(_Conn("abc"), _spec(lambda v: v == "abc"), _D)
    assert r.status == "PASSED" and r.actual_value == "abc"


def test_none_actual():
    r = specs._run_single(_Conn(None), _spec(lambda v: v is None), _D)
    assert r.status == "PASSED" and r.actual_value is None


def test_five_firm_mandatory_names_exact():
    names = {s.code for s in specs.mandatory_checks("CARD_STG", "card_auth", "txn_id", 1, 10)}
    assert names == {
        "SLA_VALIDATION", "RECORD_COUNT", "BUSINESS_DATE",
        "RECORD_COUNT_THRESHOLD", "DUPLICATE_FEED",
    }


def test_every_published_dataset_carries_all_five_mandatory():
    for layer in (specs.stage_specs(), specs.integration_specs(), specs.report_specs()):
        by_table = {}
        for s in layer:
            by_table.setdefault((s.target_schema, s.target_table), set()).add(s.code)
        for table, codes in by_table.items():
            assert {"SLA_VALIDATION", "RECORD_COUNT", "BUSINESS_DATE",
                    "RECORD_COUNT_THRESHOLD", "DUPLICATE_FEED"} <= codes, table

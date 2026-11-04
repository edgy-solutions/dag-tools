"""Numeric coercion is exact or it is the default.

Two ways a float-based coercion corrupts a payload without an error:
integers past 2**53 round to a neighbouring value, and an identifier made
of digits and one ``e`` parses as an exponent, often to ``inf``. JSON has
no ``NaN`` or ``Infinity``, so a non-finite value that does reach a plan
is caught at render time, where dry runs see it, rather than at send.

Jinja's native mode literal-parses a string result, so a plain
``{{ attr(...) }}`` over an exponent-shaped identifier would itself yield
``inf``. That parse keeps the original string whenever its result could
not travel as JSON; every other literal parse is deliberately unchanged.
"""
import datetime as dt
import decimal

import pytest

from dag_tools.otel_api_sync import (
    build_environment,
    build_functions,
    build_plan,
    load_spec,
    render_plans,
    render_value,
)
from dag_tools.otel_api_sync.functions import as_float, as_int, as_number

DEFAULT = object()
LARGE_ID = "12345678901234567891"  # > 2**53
EXPONENT_SHAPED_ID = "8e01234"     # float() reads 8 * 10**1234


@pytest.mark.parametrize("helper", [as_int, as_number])
def test_integer_strings_parse_exactly_past_float_precision(helper):
    assert helper(LARGE_ID) == 12345678901234567891
    assert helper("-" + LARGE_ID) == -12345678901234567891
    assert helper(decimal.Decimal(LARGE_ID)) == 12345678901234567891


@pytest.mark.parametrize("helper", [as_int, as_float, as_number])
def test_an_exponent_shaped_identifier_never_becomes_infinity(helper):
    assert helper(EXPONENT_SHAPED_ID, DEFAULT) is DEFAULT


@pytest.mark.parametrize("helper", [as_int, as_float, as_number])
@pytest.mark.parametrize(
    "value",
    ["nan", "NaN", "inf", "-Infinity", float("nan"), float("inf"), decimal.Decimal("NaN")],
)
def test_non_finite_input_coerces_to_the_default(helper, value):
    assert helper(value, DEFAULT) is DEFAULT


def test_as_int_refuses_a_float_parsed_value_it_cannot_represent_exactly():
    assert as_int("1234e56", DEFAULT) is DEFAULT
    assert as_int("1e3") == 1000


def test_ordinary_coercions_are_unchanged():
    assert as_number("10") == 10 and isinstance(as_number("10"), int)
    assert as_number("10.0") == 10.0 and isinstance(as_number("10.0"), float)
    assert as_number("1e3") == 1000.0 and isinstance(as_number("1e3"), float)
    assert as_number(decimal.Decimal("5")) == 5 and isinstance(as_number(decimal.Decimal("5")), int)
    assert as_number(decimal.Decimal("5.5")) == 5.5
    assert as_number(3.0) == 3.0 and isinstance(as_number(3.0), float)
    assert as_number(True) == 1
    assert as_number("", DEFAULT) is DEFAULT
    assert as_number(None, DEFAULT) is DEFAULT
    assert as_int(" 42 ") == 42
    assert as_int("3.7") == 3
    assert as_int(False) == 0
    assert as_float("81.2") == 81.2
    assert as_float("abc", DEFAULT) is DEFAULT


def test_metrics_from_prefix_skips_non_finite_values():
    metrics_from_prefix = build_functions()["metrics_from_prefix"]
    row = {"SpanAttributes": {"metric.A": "3", "metric.B": "nan", "metric.C": "inf", "metric.D": "1.5"}}

    assert metrics_from_prefix(row, "metric.") == [
        {"metricName": "A", "resultNumeric": 3},
        {"metricName": "D", "resultNumeric": 1.5},
    ]


def _spec(payload, item_key="{{ attr(item, 'SpanId') }}"):
    return load_spec(
        {
            "api": {"base_url": "https://api.test"},
            "group_by": "{{ attr(row, 'group') }}",
            "steps": [
                {
                    "id": "record",
                    "for_each": "{{ rows }}",
                    "item_key": item_key,
                    "method": "POST",
                    "path": "/api/records",
                    "payload": payload,
                }
            ],
        }
    )


def _row(**columns):
    return {
        "Timestamp": dt.datetime(2026, 1, 1, 12, 0, 0),
        "TraceId": "trace-1",
        "SpanId": "span-1",
        "SpanAttributes": {"group": "g1", "ref": EXPONENT_SHAPED_ID},
        **columns,
    }


def test_a_raw_non_finite_value_fails_the_render_and_names_the_field():
    spec = _spec({"nested": {"score": "{{ attr(item, 'Score') }}"}})

    with pytest.raises(ValueError) as excinfo:
        build_plan("g1", [_row(Score=float("nan"))], spec)

    message = str(excinfo.value)
    assert "'g1'" in message
    assert "step 'record'" in message
    assert "body.nested.score" in message


def test_render_plans_surfaces_the_same_failure_so_dry_runs_see_it():
    spec = _spec({"score": "{{ attr(item, 'Score') }}"})

    with pytest.raises(ValueError, match="non-finite"):
        render_plans([_row(Score=float("inf"))], spec, now=dt.datetime(2030, 1, 1))


def _attr(value):
    return render_value("{{ attr(row, 'x') }}", {"row": {"SpanAttributes": {"x": value}}}, build_environment())


@pytest.mark.parametrize("value", [EXPONENT_SHAPED_ID, "-5e400", "nan", "inf", "1j", "[1e999]", "{'a': 1e999}"])
def test_a_string_whose_literal_parse_json_cannot_carry_stays_a_string(value):
    assert _attr(value) == value


def test_ordinary_literal_parsing_of_a_string_result_is_unchanged():
    assert _attr("1234567") == 1234567
    assert _attr(LARGE_ID) == 12345678901234567891
    assert _attr("81.2") == 81.2
    assert _attr("True") is True
    assert _attr("[1, 2]") == [1, 2]
    assert _attr("09ab12c") == "09ab12c"


def test_a_mixed_template_is_held_to_the_same_rule():
    env = build_environment()
    assert render_value("{{ a }}{{ b }}", {"a": "8e0", "b": "1234"}, env) == "8e01234"


def test_an_exponent_shaped_identifier_reaches_the_plan_intact():
    spec = _spec({"ref": "{{ attr(item, 'ref') }}", "coerced": "{{ as_number(attr(item, 'ref')) }}"})
    spec_keyed = _spec({"ref": "{{ attr(item, 'ref') }}"}, item_key="{{ attr(item, 'ref') }}")

    body = build_plan("g1", [_row()], spec)["steps"][0]["calls"][0]["body"]
    call = build_plan("g1", [_row()], spec_keyed)["steps"][0]["calls"][0]

    assert body["ref"] == EXPONENT_SHAPED_ID
    assert body["coerced"] is None
    assert call["item_key"] == EXPONENT_SHAPED_ID

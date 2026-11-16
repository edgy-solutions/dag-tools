"""`timestamp`, `date` and `array<string>`: the three types whose obvious
coercion silently changes the value.

The module's hard edge is type fidelity and its failure mode is always
silence, so every rule here is the same stance: where the convenient reading
would alter the value, refuse it and name the column.

  * a naive timestamp names no instant, so reading it as UTC shifts a non-UTC
    source by hours, identically and invisibly;
  * a number is not a timestamp, because seconds and milliseconds cannot be
    told apart and the wrong guess lands on a plausible date;
  * a timestamp at midnight is not a date, because the exception would pass
    until the first row carried a time of day;
  * a bare string is not an array, because a string is iterable and
    `list("AB")` quietly explodes a single value into characters.

Every name in these fixtures is synthetic.
"""

from __future__ import annotations

import copy
import datetime as dt
from typing import Any, Dict

import pytest

from dag_tools.contract_mapping import (
    CONTRACT_TYPES,
    CoercionError,
    MappingPlanError,
    build_mapping_plan,
    load_contract,
    load_mapping,
)
from dag_tools.contract_mapping.emit import apply_column, coerce_value

UTC = dt.timezone.utc
PLUS_FIVE = dt.timezone(dt.timedelta(hours=5))


def _contract(column_type: str) -> Dict[str, Any]:
    return {
        "contract_id": "c_one",
        "version": "1.0.0",
        "dataset": {"urn": "urn:li:dataset:(urn:li:dataPlatform:s3,d_one,PROD)"},
        "grain": ["k_one"],
        "cadence": {"write": "whole_table", "set_by": "producer"},
        "columns": [
            {"name": "k_one", "type": "string", "required": True, "produced_since": "1"},
            {"name": "v_one", "type": column_type, "required": False, "produced_since": "1"},
        ],
        "rules": {"duplicate_key": "two rows share the stated key"},
    }


def _mapping(**column: Any) -> Dict[str, Any]:
    return {
        "contract": "c_one",
        "source": {
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:s3,d_raw,PROD)",
            "table": "t_raw",
            "columns": {"A": "string", "B": "string"},
        },
        "cadence": {"write": "whole_table", "set_by": "producer"},
        "columns": {"k_one": {"from": "A"}, "v_one": {"from": "B", **column}},
    }


def _plan(column_type: str, **column: Any):
    return build_mapping_plan(
        load_contract(_contract(column_type)), load_mapping(_mapping(**column))
    )


def test_the_three_new_types_are_accepted_and_array_is_one_token():
    """`array<string>` is one spelled token, not the start of a generic.

    Accepting `array<T>` or a bare `array` would imply element handling this
    library does not have, and a wrongly-typed array would then load happily.
    """
    for accepted in ("timestamp", "date", "array<string>"):
        assert accepted in CONTRACT_TYPES
    for refused in ("array", "array<int>", "array<T>"):
        assert refused not in CONTRACT_TYPES
        contract = load_contract(_contract(refused))
        assert contract.unknown_types == ["v_one"]
        with pytest.raises(MappingPlanError) as excinfo:
            build_mapping_plan(contract, load_mapping(_mapping()))
        assert any(refused in problem for problem in excinfo.value.problems)


def test_an_aware_timestamp_is_carried_as_utc():
    """The asset holds one zone, so two sources' rows compare by instant.

    Compared as datetimes, not strings: a string comparison would also pass a
    value that merely printed the same offset.
    """
    local = dt.datetime(2024, 3, 1, 12, 0, tzinfo=PLUS_FIVE)
    result = coerce_value(local, "timestamp", "column 'v_one'")
    assert result == dt.datetime(2024, 3, 1, 7, 0, tzinfo=UTC)
    assert result.utcoffset() == dt.timedelta(0)


def test_a_naive_timestamp_is_refused_and_the_message_names_the_column():
    """A naive value names no instant; guessing UTC shifts a source by hours.

    The shift is identical on every row, so nothing downstream could notice it.
    """
    with pytest.raises(CoercionError) as excinfo:
        coerce_value(dt.datetime(2024, 3, 1, 12, 0), "timestamp", "column 'v_one'")
    assert "column 'v_one'" in str(excinfo.value)
    assert "assume_utc" in str(excinfo.value)


def test_a_naive_timestamp_is_accepted_only_where_the_mapping_declares_it():
    """The decision is the mapping's, written down, and visible in the plan.

    There is no default: a knob that is on unless someone remembers it is how
    a non-UTC source gets read as UTC.
    """
    naive = dt.datetime(2024, 3, 1, 12, 0)
    with pytest.raises(CoercionError):
        apply_column(_plan("timestamp").columns[1], {"A": "x", "B": naive})

    plan = _plan("timestamp", assume_utc=True)
    column = plan.columns[1]
    assert column.assume_utc is True
    assert plan.columns[0].assume_utc is False
    assert apply_column(column, {"A": "x", "B": naive}) == dt.datetime(
        2024, 3, 1, 12, 0, tzinfo=UTC
    )


def test_assume_utc_on_a_non_timestamp_column_fails_the_load():
    """A knob that says nothing must not look wired up.

    On a string column it would be accepted and silently do nothing, and the
    operator would believe a conversion was happening.
    """
    with pytest.raises(MappingPlanError) as excinfo:
        _plan("string", assume_utc=True)
    assert any("assume_utc" in p and "v_one" in p for p in excinfo.value.problems)


def test_a_timestamp_from_text_requires_an_offset():
    """Text without an offset follows the naive rule, not a UTC guess."""
    with pytest.raises(CoercionError) as excinfo:
        coerce_value("2024-03-01T12:00:00", "timestamp", "column 'v_one'")
    assert "column 'v_one'" in str(excinfo.value)
    assert coerce_value("2024-03-01T12:00:00+05:00", "timestamp", "w") == dt.datetime(
        2024, 3, 1, 7, 0, tzinfo=UTC
    )
    with pytest.raises(CoercionError) as excinfo:
        coerce_value("not a time", "timestamp", "column 'v_one'")
    assert "not a time" in str(excinfo.value)


def test_timestamp_text_with_z_is_accepted():
    """`Z` is the commonest spelling of UTC; refusing it would refuse the norm."""
    assert coerce_value("2024-03-01T12:00:00Z", "timestamp", "w") == dt.datetime(
        2024, 3, 1, 12, 0, tzinfo=UTC
    )


@pytest.mark.parametrize("number", [1709294400, 1709294400000, 1.7e9, True])
def test_a_number_is_not_a_timestamp(number):
    """Seconds and milliseconds look alike, and the wrong guess is x1000.

    The message has to say why, or the refusal reads as pedantry and gets
    worked around with a cast that picks one.
    """
    with pytest.raises(CoercionError) as excinfo:
        coerce_value(number, "timestamp", "column 'v_one'")
    message = str(excinfo.value)
    assert "column 'v_one'" in message
    if not isinstance(number, bool):
        assert "seconds" in message and "milliseconds" in message


def test_a_date_is_not_a_timestamp_and_a_timestamp_is_not_a_date():
    """Neither widening is lossless: one invents a time, the other drops it.

    Midnight is refused explicitly: a midnight-only exception passes every row
    of a date-only source and fails the day a row carries a time.
    """
    with pytest.raises(CoercionError):
        coerce_value(dt.date(2024, 3, 1), "timestamp", "column 'v_one'")
    with pytest.raises(CoercionError):
        coerce_value(dt.datetime(2024, 3, 1, 0, 0, tzinfo=UTC), "date", "column 'v_one'")
    with pytest.raises(CoercionError):
        coerce_value(dt.datetime(2024, 3, 1, 9, 30), "date", "column 'v_one'")
    assert coerce_value(dt.date(2024, 3, 1), "date", "w") == dt.date(2024, 3, 1)
    assert type(coerce_value(dt.date(2024, 3, 1), "date", "w")) is dt.date


@pytest.mark.parametrize(
    "text", ["01/03/2024", "1-3-2024", "2024-3-1", "24-03-01", "20240301",
             "2024-W09-5", "2024-03-01T00:00:00", "March 1 2024"]
)
def test_a_date_parses_only_the_iso_spelling(text):
    """Ambiguous orderings are a dialect, and this library owns none.

    `20240301` and week dates are included because the standard library's
    parser accepts them; the contract is one spelling, not what it tolerates.
    """
    with pytest.raises(CoercionError):
        coerce_value(text, "date", "column 'v_one'")
    with pytest.raises(CoercionError):
        coerce_value(19783, "date", "column 'v_one'")
    assert coerce_value("2024-03-01", "date", "w") == dt.date(2024, 3, 1)


def test_a_bare_string_is_not_an_array():
    """A string is iterable, so `list(value)` explodes it into characters.

    Every single-value column would then publish as an array of letters, and
    the result looks like a plausible array. It has to be a refusal, not that.
    """
    with pytest.raises(CoercionError) as excinfo:
        coerce_value("AB", "array<string>", "column 'v_one'")
    assert "column 'v_one'" in str(excinfo.value)
    assert "list" in str(excinfo.value)


def test_an_array_element_that_is_not_a_string_is_refused_naming_its_position():
    """`str()` on an element would turn a mistakenly mapped integer column
    into an array of digit strings that looks intended."""
    with pytest.raises(CoercionError) as excinfo:
        coerce_value(["a", 7, "c"], "array<string>", "column 'v_one'")
    message = str(excinfo.value)
    assert "column 'v_one'" in message and "element 1" in message and "int" in message


def test_a_null_inside_an_array_is_refused():
    """Warehouses disagree about nullable elements, and a null is not a string.

    A stated limit rather than a silent drop, which would change the length.
    """
    with pytest.raises(CoercionError) as excinfo:
        coerce_value(["a", None], "array<string>", "column 'v_one'")
    assert "element 1" in str(excinfo.value)


@pytest.mark.parametrize("value", [{"a", "b"}, {"a": "b"}, frozenset({"a"}), iter(["a"])])
def test_a_set_is_not_an_array(value):
    """A set has no order, and an array's order is part of its value."""
    with pytest.raises(CoercionError):
        coerce_value(value, "array<string>", "column 'v_one'")


def test_an_array_of_strings_round_trips_as_a_list():
    """The accepted case, so the refusals above are not vacuous.

    A tuple comes back as a list, and the input is not aliased.
    """
    source = ["b", "a", "b"]
    result = coerce_value(source, "array<string>", "w")
    assert result == ["b", "a", "b"] and isinstance(result, list)
    assert result is not source
    assert coerce_value(("x", "y"), "array<string>", "w") == ["x", "y"]
    assert coerce_value([], "array<string>", "w") == []


@pytest.mark.parametrize("contract_type", ["timestamp", "date", "array<string>"])
def test_a_null_is_still_a_null_in_every_new_type(contract_type):
    """Nullability belongs to the column's `required`, not to the type.

    A new type that raised on None would refuse every optional column.
    """
    assert coerce_value(None, contract_type, "w") is None


_SAMPLES = {
    "string": "x",
    "numeric": 1,
    "integer": 1,
    "float": 1.5,
    "boolean": True,
    "timestamp": dt.datetime(2024, 3, 1, tzinfo=UTC),
    "date": dt.date(2024, 3, 1),
    "array<string>": ["x"],
}


@pytest.mark.parametrize("contract_type", CONTRACT_TYPES)
def test_every_contract_type_has_a_row_route(contract_type):
    """A type added to the tuple with no `coerce_value` branch fails here.

    Without it the gap shows at a deployment's first materialization, as a
    'not numeric' refusal naming nothing useful. A type with no sample is
    itself a failure, so the parametrization cannot quietly skip a new name.
    """
    assert contract_type in _SAMPLES, f"add a sample for {contract_type!r}"
    assert coerce_value(copy.copy(_SAMPLES[contract_type]), contract_type, "w") is not None

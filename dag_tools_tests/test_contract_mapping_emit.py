"""Both renderings of one mapping: the rows, and the SQL.

A mapping onto a contract has to produce the same thing whichever route it
takes -- a dbt model for a warehouse-backed source, a row operation for an
asset that is not a dbt model. So the transform vocabulary is declared rather
than being an expression in some dialect, and both renderings read the same
declaration. These tests cover the parts the shipped example does not exercise,
and the value handling the example cannot: it has no row that would be refused.

The value rules are where this gets subtle, and all three have a measurable
failure behind them:

  * `numeric` keeps the source's written form, because widening every whole
    amount to float and narrowing every whole-valued float to int each break a
    mapping's ability to reproduce its source.
  * integer text parses exactly, because `float()` first rounds anything past
    2**53.
  * a non-finite value is refused by name, because JSON carries no such number
    and a governed read refuses one.

Every name in these fixtures is synthetic.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional

import pytest

from dag_tools.contract_mapping import (
    CoercionError,
    MappingPlan,
    PlannedColumn,
    PlannedTest,
    RenderError,
    apply_to_rows,
    check_rows,
    render_dbt_artifacts,
    render_model_sql,
    render_schema_yml,
)
from dag_tools.contract_mapping.emit import apply_column, coerce_value


def _column(name: str, contract_type: str, **kwargs: Any) -> PlannedColumn:
    return PlannedColumn(name=name, contract_type=contract_type, **kwargs)


def _plan(
    columns: List[PlannedColumn],
    tests: Optional[List[PlannedTest]] = None,
    **kwargs: Any,
) -> MappingPlan:
    document: Dict[str, Any] = {
        "contract_id": "c_one",
        "contract_version": "1.0.0",
        "model_name": "m_one",
        "source_table": "t_raw",
        "columns": columns,
        "tests": tests or [],
    }
    document.update(kwargs)
    return MappingPlan(**document)


# --- values ----------------------------------------------------------------


def test_integer_text_parses_exactly_past_float_precision():
    """`float()` first would round, silently, at the top of the range.

    The same defect the OTel mapper's `as_int` was fixed for. An identifier or
    an amount past 2**53 is not exotic.
    """
    text = "9007199254740993"  # 2**53 + 1
    assert coerce_value(text, "integer", "where") == 9007199254740993
    assert float(text) != 9007199254740993


def test_a_non_finite_value_is_refused_and_the_message_names_the_field():
    """JSON carries no non-finite number and a governed read refuses one.

    Writing it into the asset defers the failure to whoever reads it, with no
    indication of which column produced it.
    """
    with pytest.raises(CoercionError) as excinfo:
        coerce_value(float("nan"), "numeric", "column 'x_one'")
    assert "x_one" in str(excinfo.value)

    with pytest.raises(CoercionError):
        coerce_value(float("inf"), "float", "column 'x_one'")


def test_a_non_whole_value_into_an_integer_column_is_refused_not_rounded():
    """Rounding here would make the asset disagree with its source silently.

    Note the asymmetry with the SQL route, which is the operator's cast and
    typically rounds. The row route refuses because it can.
    """
    with pytest.raises(CoercionError) as excinfo:
        coerce_value("1.5", "integer", "column 'x_one'")
    assert "whole number" in str(excinfo.value)

    # A whole value written fractionally is fine -- the value is not in doubt.
    assert coerce_value("2.0", "integer", "where") == 2
    assert coerce_value(2.0, "integer", "where") == 2


def test_a_numeric_column_keeps_the_sources_written_form():
    """Both obvious alternatives are measurably wrong.

    Widening everything turns every whole amount in the asset into `1.2e6`;
    narrowing every whole-valued number turns a source's `100000.0` into
    `100000`, which is enough on its own to stop a mapping reproducing its
    fixture.
    """
    assert coerce_value("100000", "numeric", "where") == 100000
    assert isinstance(coerce_value("100000", "numeric", "where"), int)

    assert isinstance(coerce_value("100000.0", "numeric", "where"), float)
    assert isinstance(coerce_value(100000, "numeric", "where"), int)
    assert isinstance(coerce_value(100000.0, "numeric", "where"), float)


def test_a_scaled_whole_result_is_a_whole_number():
    """A `Decimal` only reaches coercion from `scale`.

    There, int-ness is a property of the arithmetic rather than of anything the
    source wrote, so a whole result is a whole number.
    """
    assert coerce_value(Decimal("1200000.00"), "numeric", "where") == 1200000
    assert isinstance(coerce_value(Decimal("1200000.00"), "numeric", "where"), int)
    assert isinstance(coerce_value(Decimal("1200000.50"), "numeric", "where"), float)


def test_a_flag_in_a_numeric_column_is_refused():
    """`bool` is an `int` subclass, so this would otherwise pass as a 1 or 0.

    The mapping is reading a column the source stores as a flag, which is a
    mapping mistake and not a value to carry.
    """
    with pytest.raises(CoercionError) as excinfo:
        coerce_value(True, "numeric", "column 'x_one'")
    assert "flag" in str(excinfo.value)


def test_boolean_text_is_read_the_way_sources_write_it():
    """Sources write flags half a dozen ways, and none of them is `bool`."""
    for text in ("true", "T", " yes ", "Y", "1"):
        assert coerce_value(text, "boolean", "where") is True
    for text in ("false", "f", "no", "N", "0"):
        assert coerce_value(text, "boolean", "where") is False
    with pytest.raises(CoercionError):
        coerce_value("maybe", "boolean", "where")


def test_a_null_stays_a_null_whatever_the_column_declares():
    """Absence is not a value to coerce; `not_null` is what judges it."""
    for contract_type in ("string", "integer", "numeric", "float", "boolean"):
        assert coerce_value(None, contract_type, "where") is None


# --- the transforms the example does not exercise --------------------------


def test_constant_renders_the_same_value_both_routes():
    """A per-path constant is a column the source does not carry at all."""
    column = _column("x_one", "string", transform="constant", value="v_one")
    assert apply_column(column, {}) == "v_one"
    assert "'v_one' as x_one" in render_model_sql(_plan([column]))


def test_a_constant_with_a_quote_is_escaped_in_the_sql():
    """It is a literal in generated SQL, so the quote has to be doubled."""
    column = _column("x_one", "string", transform="constant", value="a'b")
    assert "'a''b' as x_one" in render_model_sql(_plan([column]))


def test_concat_joins_the_sources_in_order_both_routes():
    """Order is the mapping's, because a key built the other way is a different key."""
    column = _column(
        "x_one", "string", transform="concat", sources=["A", "B"], separator="-"
    )
    assert apply_column(column, {"A": "p", "B": "q"}) == "p-q"
    assert "concat(A, '-', B) as x_one" in render_model_sql(_plan([column]))


def test_concat_with_no_separator_emits_no_empty_literal():
    """`concat(A, '', B)` is valid and noisy; this is the common case."""
    column = _column("x_one", "string", transform="concat", sources=["A", "B"])
    assert apply_column(column, {"A": "p", "B": "q"}) == "pq"
    assert "concat(A, B) as x_one" in render_model_sql(_plan([column]))


def test_concat_treats_a_null_source_as_empty_rather_than_nulling_the_result():
    """Matching most dialects' `concat`, and the useful behaviour.

    A key assembled from two parts should not vanish because one part is
    absent -- the resulting value is wrong in a way `not_null` cannot see.
    """
    column = _column(
        "x_one", "string", transform="concat", sources=["A", "B"], separator="-"
    )
    assert apply_column(column, {"A": "p", "B": None}) == "p-"


def test_lower_renders_both_routes():
    column = _column("x_one", "string", transform="lower", sources=["A"])
    assert apply_column(column, {"A": "AB"}) == "ab"
    assert "lower(A) as x_one" in render_model_sql(_plan([column]))


def test_coalesce_appends_its_default_as_a_literal():
    """So a column required by the contract can be mapped from an optional one."""
    column = _column(
        "x_one", "string", transform="coalesce", sources=["A", "B"], default="v_none"
    )
    assert apply_column(column, {"A": None, "B": None}) == "v_none"
    assert "coalesce(A, B, 'v_none') as x_one" in render_model_sql(_plan([column]))


def test_scale_renders_the_factor_as_written_not_as_a_float():
    """A unit conversion of money through a binary float stops a total reconciling.

    The factor is read as a `Decimal`, so `0.01` is `0.01` in both the
    arithmetic and the generated SQL -- not `0.01000000000000000020816...`.
    """
    column = _column(
        "x_one", "numeric", transform="scale", sources=["A"], factor=Decimal("0.01")
    )
    assert apply_column(column, {"A": 120000000}) == 1200000
    assert "(A * 0.01) as x_one" in render_model_sql(_plan([column]))


def test_scale_of_a_float_source_goes_through_its_written_form():
    """`Decimal(0.1)` is not `Decimal('0.1')`, and the difference compounds."""
    column = _column(
        "x_one", "numeric", transform="scale", sources=["A"], factor=Decimal("0.01")
    )
    assert apply_column(column, {"A": 10.1}) == 0.101


def test_a_row_missing_a_source_column_names_the_column():
    """The row route's counterpart of the plan's source-schema check."""
    column = _column("x_one", "string", transform="copy", sources=["A"])
    with pytest.raises(RenderError) as excinfo:
        apply_column(column, {"B": "p"})
    assert "A" in str(excinfo.value) and "x_one" in str(excinfo.value)


def test_output_column_order_is_the_contracts_not_the_sources():
    """Two mappings onto one contract must produce the same shape."""
    plan = _plan(
        [
            _column("x_two", "string", sources=["B"]),
            _column("x_one", "string", sources=["A"]),
        ]
    )
    produced = apply_to_rows(plan, [{"A": "p", "B": "q"}])
    assert list(produced[0]) == ["x_two", "x_one"]


# --- the checks ------------------------------------------------------------


def _checked_plan() -> MappingPlan:
    columns = [
        _column("k_one", "string", sources=["A"], required=True),
        _column("k_two", "string", sources=["B"], required=True),
        _column("m_one", "numeric", sources=["C"], constant_within=["k_one"]),
    ]
    tests = [
        PlannedTest(rule_id="r_missing", kind="column_present", columns=["k_one", "m_one"]),
        PlannedTest(rule_id="r_type", kind="types_enforced", columns=["k_one", "m_one"]),
        PlannedTest(rule_id="r_null", kind="not_null", columns=["k_one", "k_two"]),
        PlannedTest(rule_id="r_dup", kind="key_unique", columns=["k_one", "k_two"]),
        PlannedTest(
            rule_id="r_varies", kind="constant_within", columns=["m_one"], within=["k_one"]
        ),
        PlannedTest(
            rule_id="r_period",
            kind="period_in_calendar",
            columns=["k_two"],
            values=["p1", "p2"],
        ),
        PlannedTest(
            rule_id="r_nonfinite",
            kind="non_finite",
            columns=["m_one"],
            predicate="{column} != {column}",
        ),
    ]
    return _plan(columns, tests)


def test_rows_that_satisfy_every_invariant_produce_no_failures():
    """The baseline the failures below are each one edit from."""
    rows = [
        {"k_one": "a", "k_two": "p1", "m_one": 1},
        {"k_one": "a", "k_two": "p2", "m_one": 1},
    ]
    assert check_rows(_checked_plan(), rows) == []


def test_every_failing_invariant_is_returned_at_once_rather_than_raised():
    """An asset check wants every failing rule, and the caller decides the gate.

    Raising on the first would make one bad load take as many runs to diagnose
    as it has problems.
    """
    rows = [
        {"k_one": "a", "k_two": "p1", "m_one": 1},
        {"k_one": "a", "k_two": "p1", "m_one": 2},
        {"k_one": None, "k_two": "p9", "m_one": float("nan")},
    ]
    failures = check_rows(_checked_plan(), rows)
    by_rule = {failure.rule_id: failure for failure in failures}
    assert set(by_rule) == {"r_null", "r_dup", "r_varies", "r_period", "r_nonfinite"}

    assert by_rule["r_null"].columns == ["k_one"]
    assert by_rule["r_dup"].columns == ["k_one", "k_two"]
    assert by_rule["r_varies"].columns == ["m_one"]
    assert "p9" not in by_rule["r_period"].detail  # the count, not the values
    assert by_rule["r_period"].row_count == 1


def test_a_missing_column_is_reported_under_the_contracts_rule():
    """The rows came from somewhere this library did not render.

    `check_rows` is the asset-check route, so it is handed rows it did not
    produce and cannot assume the shape.
    """
    failures = check_rows(_checked_plan(), [{"k_one": "a", "k_two": "p1"}])
    missing = next(failure for failure in failures if failure.rule_id == "r_missing")
    assert missing.columns == ["m_one"]


def test_a_flag_in_a_numeric_column_is_caught_by_the_type_check():
    """`bool` is an `int` subclass, so a naive `isinstance` check would pass it."""
    rows = [{"k_one": "a", "k_two": "p1", "m_one": True}]
    failures = check_rows(_checked_plan(), rows)
    assert any(failure.rule_id == "r_type" for failure in failures)


def test_a_null_does_not_trip_the_type_check():
    """Absence is `not_null`'s business; typing a null is a second failure
    for one fact."""
    rows = [{"k_one": "a", "k_two": "p1", "m_one": None}]
    failures = check_rows(_checked_plan(), rows)
    assert [failure.rule_id for failure in failures] == []


# --- the dbt artifacts -----------------------------------------------------


def test_the_contract_is_enforced_only_when_every_column_has_a_warehouse_type():
    """A contract naming some columns and not others reads as a passing contract.

    dbt would enforce the named ones and silently accept anything in the rest.
    """
    typed = [
        _column("x_one", "string", sources=["A"], warehouse_type="t_string"),
        _column("x_two", "string", sources=["B"], warehouse_type="t_string"),
    ]
    assert "enforced: true" in render_schema_yml(_plan(typed))

    partial = [typed[0], _column("x_two", "string", sources=["B"])]
    assert "enforced: true" not in render_schema_yml(_plan(partial))


def test_a_not_null_test_is_named_after_the_rule_it_fails_under():
    """So a `dbt test` failure and a governed read's refusal name one rule.

    Otherwise an operator maps dbt's test name to the contract's rule by hand,
    every time, under time pressure.
    """
    columns = [
        _column("x_one", "string", sources=["A"], warehouse_type="t_string", required=True)
    ]
    tests = [PlannedTest(rule_id="r_null", kind="not_null", columns=["x_one"])]
    rendered = render_schema_yml(_plan(columns, tests))
    assert "name: m_one__r_null__x_one" in rendered
    assert "data_type: t_string" in rendered


def test_every_singular_test_carries_its_rule_id_as_a_column():
    """A failing row set that does not say which rule it broke is a puzzle."""
    files = render_dbt_artifacts(_checked_plan())
    singular = {name: text for name, text in files.items() if name.startswith("tests/")}
    # not_null rides on schema.yml; presence and types ride on the contract.
    assert len(singular) == 4
    for name, text in singular.items():
        assert "-- rule: r_" in text
        assert "as rule_id" in text
        assert "{{ ref('m_one') }}" in text


def test_a_mapping_with_no_source_table_cannot_render_a_model():
    """A mapping with no table is a rows-mode mapping.

    Rendering `from ` with nothing after it would produce a file dbt accepts
    into the project and fails on at build time.
    """
    plan = _plan([_column("x_one", "string", sources=["A"])], source_table=None)
    with pytest.raises(RenderError) as excinfo:
        render_model_sql(plan)
    assert "rows-mode" in str(excinfo.value)


def test_the_model_header_names_the_contract_and_tells_the_reader_not_to_edit():
    """Generated SQL in a project someone maintains by hand needs both."""
    sql = render_model_sql(_checked_plan())
    assert sql.startswith("-- generated from contract c_one 1.0.0")
    assert "edit the mapping, not this file" in sql


def test_the_schema_yml_records_the_asset_of_record():
    """The physical platform's dataset URN, per the 11-05 ruling.

    The Dagster asset attaches as lineage; the contract's dataset is the thing
    of record, and the generated model's description is where a reader of the
    dbt project finds it.
    """
    plan = _plan(
        [_column("x_one", "string", sources=["A"])],
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:s3,d_one,PROD)",
    )
    assert "dataPlatform:s3,d_one,PROD" in render_schema_yml(plan)
    assert "dataPlatform:dagster" not in render_schema_yml(plan)

"""What `build_mapping_plan` refuses, and what it only reports.

The line the plan draws is one question: would the emitted artifact be wrong?
A mapping that produces a column outside its contract, or fails to produce one
a governed read selects, emits SQL that cannot be used -- those are refusals. A
mapping that cannot get a test written because the warehouse's type names are
not declared emits less than it could, which is a note. Getting that line wrong
in either direction is the failure mode worth testing: refuse too much and no
mapping ever loads, refuse too little and a broken model reaches a warehouse.

Refusals are also **collected**, not raised one at a time. An operator fixing a
mapping file wants the list.

Every name in these fixtures is synthetic.
"""

from __future__ import annotations

import copy
from typing import Any, Dict

import pytest

from dag_tools.contract_mapping import (
    MappingPlanError,
    build_mapping_plan,
    load_contract,
    load_mapping,
)

# A contract with one of everything the plan reacts to: a grain, a scope, a
# period, a required column, a forward-declared column, a constant-within
# invariant, and a rules block.
CONTRACT: Dict[str, Any] = {
    "contract_id": "c_one",
    "version": "1.0.0",
    "dataset": {"urn": "urn:li:dataset:(urn:li:dataPlatform:s3,d_one,PROD)"},
    "grain": ["k_one", "k_two"],
    "scope": "s_one",
    "period": {"column": "k_two", "values": ["p1", "p2"]},
    "cadence": {"write": "whole_table", "set_by": "producer"},
    "columns": [
        {"name": "s_one", "type": "string", "required": True, "produced_since": "1"},
        {"name": "k_one", "type": "string", "required": True, "produced_since": "1"},
        {"name": "k_two", "type": "string", "required": True, "produced_since": "1"},
        {
            "name": "m_one",
            "type": "numeric",
            "required": False,
            "produced_since": "1",
            "constant_within": ["k_one"],
        },
        {"name": "m_two", "type": "integer", "required": False, "produced_since": None},
    ],
    "rules": {
        "missing_column": "a required column is absent",
        "wrong_type": "a column is not the declared type",
        "null_value": "a required column is null",
        "duplicate_k_one_k_two": "two rows share the stated key",
        "period_outside_calendar": "a period is not in the calendar",
        "m_one_not_unique": "m_one varies inside one k_one",
        "non_finite": "a numeric value is not finite",
    },
}

MAPPING: Dict[str, Any] = {
    "contract": "c_one",
    "source": {
        "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:s3,d_raw,PROD)",
        "table": "t_raw",
        "columns": {"A": "string", "B": "string", "C": "string", "D": "integer"},
    },
    "cadence": {"write": "whole_table", "set_by": "producer"},
    "columns": {
        "s_one": {"from": "A"},
        "k_one": {"from": "B"},
        "k_two": {"from": "C"},
        "m_one": {"from": "D"},
    },
    "warehouse_types": {
        "string": "t_string",
        "integer": "t_integer",
        "numeric": "t_numeric",
        "float": "t_float",
        "boolean": "t_boolean",
    },
    "non_finite_predicate": "{column} != {column}",
}


def _plan(contract: Dict[str, Any] = None, mapping: Dict[str, Any] = None):
    return build_mapping_plan(
        load_contract(contract if contract is not None else copy.deepcopy(CONTRACT)),
        load_mapping(mapping if mapping is not None else copy.deepcopy(MAPPING)),
    )


def _mapping(**changes: Any) -> Dict[str, Any]:
    document = copy.deepcopy(MAPPING)
    document.update(changes)
    return document


def _problems(contract=None, mapping=None):
    with pytest.raises(MappingPlanError) as excinfo:
        _plan(contract, mapping)
    return excinfo.value.problems


def test_the_baseline_fixture_plans_cleanly_with_nothing_to_report():
    """Every test below is a single edit away from this, so it has to be clean.

    A fixture that already carries notes makes every note assertion below
    ambiguous about which edit produced it.
    """
    plan = _plan()
    assert plan.notes == []
    assert plan.unmapped_declared == ["m_two"]
    assert [test.kind for test in plan.tests] == [
        "column_present",
        "types_enforced",
        "not_null",
        "key_unique",
        "constant_within",
        "period_in_calendar",
        "non_finite",
    ]


# --- refusals --------------------------------------------------------------


def test_a_mapping_paired_with_the_wrong_contract_is_refused():
    """The pairing is the one thing nothing downstream can check.

    A mapping and a contract are two files; whatever hands them to each other
    can hand over the wrong pair, and every column name might still line up.
    """
    problems = _problems(mapping=_mapping(contract="c_other"))
    assert any("c_other" in problem and "c_one" in problem for problem in problems)


def test_a_column_outside_the_contract_is_refused():
    """A produced column no governed read selects is dead weight at best.

    At worst it is a column someone reads anyway, outside the contract, which
    is the thing a contract exists to stop.
    """
    columns = dict(MAPPING["columns"])
    columns["x_unknown"] = {"from": "A"}
    problems = _problems(mapping=_mapping(columns=columns))
    assert any("x_unknown" in problem for problem in problems)


def test_an_unproduced_required_column_is_refused():
    """`required` means every governed read selects it.

    So a mapping that omits one produces a model no read can be served from,
    and it has to fail where an operator is editing the mapping.
    """
    columns = dict(MAPPING["columns"])
    del columns["s_one"]
    problems = _problems(mapping=_mapping(columns=columns))
    assert any("s_one" in problem for problem in problems)


def test_an_unproduced_grain_column_is_refused():
    """The key-uniqueness test is stated over exactly the grain.

    Drop a grain column and the test is either unwritable or written over a
    narrower key, which passes while the real key duplicates.
    """
    contract = copy.deepcopy(CONTRACT)
    # Make the grain column optional, so only the grain rule can refuse it.
    for column in contract["columns"]:
        if column["name"] == "k_one":
            column["required"] = False
    columns = dict(MAPPING["columns"])
    del columns["k_one"]
    problems = _problems(contract=contract, mapping=_mapping(columns=columns))
    assert any("k_one" in problem and "grain" in problem for problem in problems)


def test_an_unproduced_scope_column_is_refused():
    """A read filters to one scope value; without the column it cannot.

    The resulting model would serve every scope to every reader.
    """
    contract = copy.deepcopy(CONTRACT)
    for column in contract["columns"]:
        if column["name"] == "s_one":
            column["required"] = False
    columns = dict(MAPPING["columns"])
    del columns["s_one"]
    problems = _problems(contract=contract, mapping=_mapping(columns=columns))
    assert any("s_one" in problem and "scope" in problem for problem in problems)


def test_a_cadence_disagreement_is_refused_because_it_fails_silently():
    """Incremental against a whole-table contract shows up as data, not error.

    A reader expecting one and served the other sees stale rows or duplicates.
    Nothing raises, which is exactly why this one cannot be a note.
    """
    problems = _problems(
        mapping=_mapping(cadence={"write": "incremental", "set_by": "producer"})
    )
    assert any("incremental" in problem for problem in problems)


def test_a_source_column_the_schema_does_not_carry_is_refused():
    """The whole reason the mapping may declare the source's schema.

    With it, this fails at the code location load. Without it, the same
    mapping fails inside a run at whatever hour the schedule fires.
    """
    columns = copy.deepcopy(MAPPING["columns"])
    columns["s_one"] = {"from": "Z_ABSENT"}
    problems = _problems(mapping=_mapping(columns=columns))
    assert any("Z_ABSENT" in problem for problem in problems)


def test_a_contract_type_this_library_cannot_render_is_refused():
    """An unknown type cannot be cast, tested, or checked in rows.

    Emitting the column anyway would produce SQL whose type nothing verified.
    """
    contract = copy.deepcopy(CONTRACT)
    for column in contract["columns"]:
        if column["name"] == "m_one":
            column["type"] = "money"
    problems = _problems(contract=contract)
    assert any("money" in problem for problem in problems)


def test_scaling_a_non_numeric_column_is_refused():
    """Scaling a string is a mapping mistake, not a cast.

    The transform would render as arithmetic over text, which some dialects
    will happily coerce -- so the warehouse is not a backstop here.
    """
    columns = copy.deepcopy(MAPPING["columns"])
    columns["k_one"] = {"from": "B", "transform": "scale", "factor": "0.01"}
    problems = _problems(mapping=_mapping(columns=columns))
    assert any("k_one" in problem and "scal" in problem for problem in problems)


def test_every_problem_is_reported_at_once():
    """An operator fixing a mapping file wants the list, not the first line.

    Raising on the first refusal turns one edit pass into as many passes as
    there are mistakes.
    """
    contract = copy.deepcopy(CONTRACT)
    for column in contract["columns"]:
        column["required"] = False
    columns = {"x_unknown": {"from": "A"}, "m_one": {"from": "Z_ABSENT"}}
    problems = _problems(contract=contract, mapping=_mapping(columns=columns))
    assert len(problems) >= 5
    joined = " | ".join(problems)
    for expected in ("x_unknown", "Z_ABSENT", "grain", "scope", "period"):
        assert expected in joined


def test_an_out_of_contract_column_is_refused_once_not_twice():
    """Its source column is not also checked, because the column is already out.

    A second problem about where a column nobody will read gets its value from
    is noise in a list an operator has to work through.
    """
    columns = {**copy.deepcopy(MAPPING["columns"]), "x_unknown": {"from": "Z_ABSENT"}}
    problems = _problems(mapping=_mapping(columns=columns))
    assert len(problems) == 1
    assert "x_unknown" in problems[0]


# --- notes -----------------------------------------------------------------


def _notes(contract=None, mapping=None):
    return " | ".join(_plan(contract, mapping).notes)


def test_a_mapping_with_no_contract_declared_is_noted_not_refused():
    """A mapping file may omit the id; then nothing checks the pairing.

    Refusing would break every mapping handed its contract by the component
    rather than naming it, so the plan says what is unchecked instead.
    """
    mapping = _mapping()
    del mapping["contract"]
    assert "nothing checks that it was paired" in _notes(mapping=mapping)


def test_a_missing_source_schema_is_noted_so_the_gap_is_visible():
    """Without a schema the source-column checks cannot run at all.

    That is a legitimate choice -- the catalog has the real schema -- but an
    operator should not have to infer that the load-time check did not happen.
    """
    source = dict(MAPPING["source"])
    source.pop("columns")
    assert "unverified until materialization" in _notes(
        mapping=_mapping(source=source)
    )


def test_producing_a_forward_declared_column_is_noted_as_a_contract_edit():
    """This mapping is the release that starts producing it.

    So `produced_since` wants setting on the contract, and the operator is the
    only one who can do it.
    """
    columns = dict(MAPPING["columns"])
    columns["m_two"] = {"from": "D"}
    notes = _notes(mapping=_mapping(columns=columns))
    assert "m_two" in notes and "produced_since" in notes


def test_a_period_column_disagreement_is_noted_with_the_mapping_winning():
    """The mapping is nearer the data, so it wins -- visibly.

    Silently preferring either one is how a period test ends up stated over a
    column the model does not have.
    """
    notes = _notes(mapping=_mapping(period={"key": "k_one"}))
    assert "k_one" in notes and "k_two" in notes


def test_a_cadence_set_by_difference_is_only_a_note():
    """Unlike `write`, who sets the cadence changes no rows.

    It is a governance fact worth surfacing, not a reason to refuse a model.
    """
    notes = _notes(
        mapping=_mapping(cadence={"write": "whole_table", "set_by": "consumer"})
    )
    assert "set_by" in notes


def test_no_warehouse_types_means_no_enforced_contract_and_says_so():
    """This library does not know what a warehouse calls a string.

    A guess would emit a model contract that fails for a reason with nothing
    to do with the data, so the block is required to get one -- and its
    absence has to be visible, because it silently drops two rules.
    """
    mapping = _mapping()
    del mapping["warehouse_types"]
    plan = _plan(mapping=mapping)
    assert "enforces no contract" in " | ".join(plan.notes)
    assert "column_present" not in [test.kind for test in plan.tests]
    assert "types_enforced" not in [test.kind for test in plan.tests]


def test_one_missing_warehouse_type_drops_the_contract_rather_than_half_of_it():
    """A model contract naming some columns and not others is not a contract.

    dbt would enforce the named ones and silently accept anything in the rest,
    which reads as a passing contract.
    """
    types = dict(MAPPING["warehouse_types"])
    del types["numeric"]
    plan = _plan(mapping=_mapping(warehouse_types=types))
    assert "numeric" in " | ".join(plan.notes)
    assert "types_enforced" not in [test.kind for test in plan.tests]


def test_a_type_changing_transform_with_no_cast_is_noted():
    """The defect this seam was added for, stated as a note.

    `scale` on an integer column renders as a multiplication, which most
    dialects widen -- so an enforced contract refuses the model. The library
    cannot supply the cast (it does not own the dialect) so it names the
    columns that need one.
    """
    contract = copy.deepcopy(CONTRACT)
    for column in contract["columns"]:
        if column["name"] == "m_one":
            column["type"] = "integer"
    columns = copy.deepcopy(MAPPING["columns"])
    columns["m_one"] = {"from": "D", "transform": "scale", "factor": "0.01"}
    notes = _notes(contract=contract, mapping=_mapping(columns=columns))
    assert "m_one" in notes and "cast_templates" in notes


def test_a_declared_cast_clears_the_note_and_reaches_the_planned_column():
    """The note is actionable, so the action has to work."""
    contract = copy.deepcopy(CONTRACT)
    for column in contract["columns"]:
        if column["name"] == "m_one":
            column["type"] = "integer"
    columns = copy.deepcopy(MAPPING["columns"])
    columns["m_one"] = {"from": "D", "transform": "scale", "factor": "0.01"}
    plan = _plan(
        contract=contract,
        mapping=_mapping(
            columns=columns, cast_templates={"integer": "cast({expr} as t_integer)"}
        ),
    )
    assert plan.notes == []
    planned = next(column for column in plan.columns if column.name == "m_one")
    assert planned.cast_template == "cast({expr} as t_integer)"


def test_a_constant_within_an_unproduced_column_is_noted_not_tested():
    """The invariant is real but untestable in this model.

    Emitting the test anyway would group by a column that is not there.
    """
    contract = copy.deepcopy(CONTRACT)
    for column in contract["columns"]:
        if column["name"] == "m_one":
            column["constant_within"] = ["m_two"]
    plan = _plan(contract=contract)
    assert "m_two" in " | ".join(plan.notes)
    assert "constant_within" not in [test.kind for test in plan.tests]


def test_a_named_but_unlisted_calendar_gets_no_test_and_says_why():
    """A test against a vocabulary NAME would pass without checking anything.

    This is the shape of vacuous pass the whole module is written to avoid, so
    it is a note plus a missing test rather than an emitted test.
    """
    contract = copy.deepcopy(CONTRACT)
    contract["period"] = {"column": "k_two", "vocabulary": "V_ONE"}
    plan = _plan(contract=contract)
    notes = " | ".join(plan.notes)
    assert "V_ONE" in notes and "pass without checking anything" in notes
    assert "period_in_calendar" not in [test.kind for test in plan.tests]


def test_listing_the_calendar_in_the_mapping_is_what_gets_the_test_written():
    """The documented way out of the note above."""
    contract = copy.deepcopy(CONTRACT)
    contract["period"] = {"column": "k_two", "vocabulary": "V_ONE"}
    plan = _plan(
        contract=contract, mapping=_mapping(period={"key": "k_two", "values": ["p1"]})
    )
    test = next(test for test in plan.tests if test.kind == "period_in_calendar")
    assert test.values == ["p1"]


def test_no_non_finite_predicate_means_no_non_finite_test_and_says_so():
    """Every adapter spells an infinity test differently.

    A guessed predicate would silently match nothing, which is worse than no
    test at all because it reads as covered.
    """
    mapping = _mapping()
    del mapping["non_finite_predicate"]
    plan = _plan(mapping=mapping)
    assert "non_finite_predicate" in " | ".join(plan.notes)
    assert "non_finite" not in [test.kind for test in plan.tests]


def test_a_test_under_a_rule_the_contract_does_not_declare_is_noted():
    """A failure naming a rule no reader refuses under is not a gate.

    The test still gets emitted -- it catches real breakage -- but the plan
    says the id is outside the contract.
    """
    contract = copy.deepcopy(CONTRACT)
    del contract["rules"]["duplicate_k_one_k_two"]
    plan = _plan(contract=contract)
    notes = " | ".join(plan.notes)
    assert "duplicate_k_one_k_two" in notes
    test = next(test for test in plan.tests if test.kind == "key_unique")
    assert test.declared_in_contract is False


# --- rule ids --------------------------------------------------------------


def test_the_key_uniqueness_id_is_derived_from_the_grain_not_written_down():
    """So no column name has to appear in this library.

    The contract spells the id after its own grain; the derivation reproduces
    that spelling from the grain it declares.
    """
    plan = _plan()
    test = next(test for test in plan.tests if test.kind == "key_unique")
    assert test.rule_id == "duplicate_k_one_k_two"
    assert test.declared_in_contract is True


def test_the_mapping_can_override_a_rule_id_so_the_generic_ids_are_not_binding():
    """The escape hatch that keeps this library's id list from being a contract.

    A deployment whose contract spells a rule differently sets it here rather
    than needing a change in dag-tools.
    """
    plan = _plan(mapping=_mapping(rule_ids={"not_null": "null_value"}))
    test = next(test for test in plan.tests if test.kind == "not_null")
    assert test.rule_id == "null_value"
    assert test.declared_in_contract is True


def test_an_override_naming_a_rule_the_contract_lacks_is_still_flagged():
    """An override is not an assertion that the contract declares the id."""
    plan = _plan(mapping=_mapping(rule_ids={"not_null": "r_absent"}))
    test = next(test for test in plan.tests if test.kind == "not_null")
    assert test.rule_id == "r_absent"
    assert test.declared_in_contract is False


# --- the hash --------------------------------------------------------------


def test_the_plan_hash_is_stable_across_rebuilds():
    """It goes into the rendered SQL, so an unstable hash churns every file."""
    assert _plan().plan_hash == _plan().plan_hash


def test_the_plan_hash_covers_the_columns_and_tests_and_not_the_notes():
    """The hash answers "would the emitted bytes differ?".

    A note changes what an operator is told, not what is written, so folding
    notes in would rewrite every generated file on a message edit.
    """
    baseline = _plan()
    source = dict(MAPPING["source"])
    source.pop("columns")
    noted = _plan(mapping=_mapping(source=source))
    assert noted.notes
    assert noted.plan_hash == baseline.plan_hash

    changed = _plan(
        mapping=_mapping(
            columns={
                **copy.deepcopy(MAPPING["columns"]),
                "m_one": {"from": "D", "transform": "scale", "factor": "2"},
            },
            cast_templates={"numeric": "cast({expr} as t_numeric)"},
        )
    )
    assert changed.plan_hash != baseline.plan_hash

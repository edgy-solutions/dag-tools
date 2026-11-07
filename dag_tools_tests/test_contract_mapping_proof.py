"""The proof: this mapping reproduces the fixture dag-tools already ships.

A mapping engine is only worth anything if a mapping file written against a
real contract produces the rows that contract describes, down to the types. So
this module takes the contract the example vendors, the mapping the example
ships, and the fixture CSV already in this repo, derives a raw source table by
**inverting every transform the plan declares**, and asserts the plan maps it
back to the fixture exactly.

The inversion is generic -- it reads the plan, not a list of columns -- for two
reasons. It names no column, so the test stays correct when the example's
mapping changes; and it fails if the plan's description of a transform ever
stops matching what the row renderer does with it, which is the one
disagreement that would make a dry run a lie.

Every name here is from dag-tools' own synthetic sandbox fixture.
"""

from __future__ import annotations

import csv
import os
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
import yaml

from dag_tools.contract_mapping import (
    MappingPlan,
    apply_to_rows,
    build_mapping_plan,
    check_rows,
    load_contract,
    load_mapping,
    render_dbt_artifacts,
)

REPO = Path(__file__).resolve().parents[1]
EXAMPLE = REPO / "examples" / "contract_mapping"
CONTRACT_FILE = EXAMPLE / "contracts" / "ev_fixture.yaml"
MAPPING_FILE = (
    EXAMPLE / "dagster_home" / "components" / "contract_mapping" / "mapping.yaml"
)
FIXTURE_CSV = REPO / "dag_tools" / "user_deployment" / "demo_data" / "ev_fixture.csv"


@pytest.fixture(scope="module")
def plan() -> MappingPlan:
    contract = load_contract(yaml.safe_load(CONTRACT_FILE.read_text(encoding="utf-8")))
    mapping = load_mapping(yaml.safe_load(MAPPING_FILE.read_text(encoding="utf-8")))
    return build_mapping_plan(contract, mapping)


# --- the fixture, as the fixture is written --------------------------------


def _fixture_rows() -> List[Dict[str, Any]]:
    """The shipped CSV, typed by what the file itself writes.

    The engine's rule for a `numeric` column is that the source's written form
    decides, so this reads the written form off the text rather than asking the
    engine -- otherwise the proof would be checking the engine against itself.
    """
    contract = load_contract(yaml.safe_load(CONTRACT_FILE.read_text(encoding="utf-8")))
    types = {column.name: column.type for column in contract.columns}

    def typed(name: str, text: str) -> Any:
        kind = types[name]
        if kind == "string":
            return text
        if kind == "integer":
            return int(text)
        if kind == "float":
            return float(text)
        return float(text) if "." in text else int(text)

    with FIXTURE_CSV.open(newline="", encoding="utf-8") as handle:
        return [
            {name: typed(name, value) for name, value in row.items()}
            for row in csv.DictReader(handle)
        ]


def _wrong(value: Any) -> Any:
    """A value that is not the right one, in the same shape.

    Put behind the value a `coalesce` should pick, so an implementation that
    took the last non-null rather than the first would be caught.
    """
    if isinstance(value, str):
        return value + "-NOT-THIS-ONE"
    if isinstance(value, bool):
        return not value
    if isinstance(value, (int, float, Decimal)):
        return value + 1
    return "NOT-THIS-ONE"


def _invert(plan: MappingPlan, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build the raw source table the plan would have had to read.

    One branch per transform the mapping vocabulary carries, driven off the
    plan. Anything the vocabulary grows that is not inverted here fails loudly
    rather than quietly dropping a column from the proof.
    """
    raw_rows: List[Dict[str, Any]] = []
    for index, row in enumerate(rows):
        raw: Dict[str, Any] = {}
        for column in plan.columns:
            value = row[column.name]
            if column.transform == "constant":
                continue
            if column.transform == "copy":
                raw[column.sources[0]] = value
            elif column.transform == "trim":
                raw[column.sources[0]] = "  {} ".format(value)
            elif column.transform == "upper":
                raw[column.sources[0]] = value.lower()
            elif column.transform == "lower":
                raw[column.sources[0]] = value.upper()
            elif column.transform == "concat":
                parts = str(value).split(column.separator)
                assert len(parts) == len(column.sources), (
                    column.name + ": the fixture value does not split into the "
                    "mapping's sources, so this proof cannot invert it"
                )
                for source, part in zip(column.sources, parts):
                    raw[source] = part
            elif column.transform == "coalesce":
                # Rotate which source carries the value, so both the
                # first-source path and the fallback path are exercised over
                # the fixture rather than only one of them.
                carrier = index % len(column.sources)
                for position, source in enumerate(column.sources):
                    if position < carrier:
                        raw[source] = None
                    elif position == carrier:
                        raw[source] = value
                    else:
                        raw[source] = _wrong(value)
            elif column.transform == "scale":
                assert column.factor is not None
                raw[column.sources[0]] = int(Decimal(str(value)) / column.factor)
            else:  # pragma: no cover - a transform this proof has not learned
                raise AssertionError(
                    column.name + ": this proof cannot invert transform "
                    + repr(column.transform)
                    + "; teach it before shipping the transform"
                )
        raw_rows.append(raw)
    return raw_rows


# --- the proof -------------------------------------------------------------


def test_the_mapping_reproduces_the_fixture_exactly(plan):
    """Values AND types, over every row of the shipped fixture.

    Types are compared because they are where a mapping engine goes wrong
    quietly: a `numeric` column widened to float turns every whole amount in
    the asset into `1.2e6`, which reads as the same number and is not.
    """
    expected = _fixture_rows()
    produced = apply_to_rows(plan, _invert(plan, expected))

    assert len(produced) == len(expected) == 54

    mismatches = []
    for index, (got, want) in enumerate(zip(produced, expected)):
        for name in got:
            if got[name] != want[name] or type(got[name]) is not type(want[name]):
                mismatches.append(
                    "row {} {}: got {!r} ({}), fixture has {!r} ({})".format(
                        index,
                        name,
                        got[name],
                        type(got[name]).__name__,
                        want[name],
                        type(want[name]).__name__,
                    )
                )
    assert mismatches == []


def test_the_plan_produces_every_fixture_column_and_no_others(plan):
    """The mapping's output shape is the fixture's shape.

    A column the plan invents would not be caught by the row comparison above
    (which walks what was produced), and a column it drops would not be caught
    by a comparison that walks the fixture.
    """
    expected = _fixture_rows()
    assert set(plan.output_column_names) == set(expected[0])


def test_the_produced_rows_satisfy_every_test_the_plan_emits(plan):
    """The emitted tests pass over the rows the mapping produces.

    This is the half that catches a plan whose tests are stated over the wrong
    columns: a key-uniqueness test over the scope rather than the grain passes
    on nothing, and the fixture is 54 rows across one program.
    """
    produced = apply_to_rows(plan, _invert(plan, _fixture_rows()))
    assert check_rows(plan, produced) == []


def test_every_emitted_test_fails_under_a_rule_the_contract_declares(plan):
    """A test failing under an id no reader refuses under is not a gate.

    The contract states its rules; a failure that names something outside that
    list cannot be acted on by anyone reading the contract. The key-uniqueness
    id is derived structurally from the grain rather than written down in this
    library, so this also pins that derivation against the real document.
    """
    undeclared = [
        (test.kind, test.rule_id)
        for test in plan.tests
        if not test.declared_in_contract
    ]
    assert undeclared == []
    assert len(plan.tests) == 11


def test_the_example_mapping_leaves_the_plan_with_nothing_to_report(plan):
    """No notes on the shipped example.

    Every note names something the plan did NOT emit -- a test not written, a
    contract not enforced. A worked example carrying notes is an example that
    quietly demonstrates less than it looks like it does.
    """
    assert plan.notes == []


def test_the_contracts_forward_declared_columns_are_reported_not_demanded(plan):
    """A column declared but not yet produced is not a mapping error.

    The contract declares columns ahead of anything producing them so readers
    can be written against them. The plan has to let a mapping omit those
    while still making the forward edge visible.
    """
    assert plan.unmapped_declared == ["status_period", "eac_reported"]


def test_the_rendered_model_selects_the_columns_in_contract_order(plan):
    """Rendering is the other half; the rows above only prove the row route.

    Two mappings onto one contract must produce the same column order, so the
    order is the contract's and not the mapping file's.
    """
    sql = render_dbt_artifacts(plan)[plan.model_name + ".sql"]

    positions = []
    for name in plan.output_column_names:
        with_comma = " as {},\n".format(name)
        last = " as {}\n".format(name)
        positions.append(
            sql.index(with_comma) if with_comma in sql else sql.index(last)
        )
    assert positions == sorted(positions)
    assert plan.source_table in sql
    assert plan.plan_hash in sql


def test_the_scaled_columns_carry_a_cast_so_the_contract_can_be_enforced(plan):
    """The bug this seam exists for, pinned on the real document.

    A scaled integer renders as a multiplication, which most dialects widen to
    a fractional type -- and an enforced model contract then refuses the model
    for a reason that has nothing to do with the data. The cast has to wrap the
    whole expression, not the source column: what needs correcting is the type
    the transform produced.
    """
    scaled = [column for column in plan.columns if column.transform == "scale"]
    assert scaled, "the example no longer exercises `scale`"

    sql = render_dbt_artifacts(plan)[plan.model_name + ".sql"]
    for column in scaled:
        assert column.cast_template, column.name + " is scaled with no cast"
        line = next(
            line
            for line in sql.splitlines()
            if line.rstrip(",").endswith(" as " + column.name)
        )
        # The cast is outside the arithmetic, not around the source column.
        assert line.strip().startswith("cast((")
        assert column.sources[0] in line


# --- drift ----------------------------------------------------------------


def _upstream_contract() -> Optional[Path]:
    """Where the contract this example vendors is maintained, if it is here.

    The contract is governed in the agent repo; dag-tools vendors a copy so
    the example and this proof run with no second checkout. That makes drift
    possible, so when the upstream copy IS on this machine it is compared.
    Skipped rather than guessed at: a canary that silently passes when it
    cannot find its target is worse than no canary.
    """
    override = os.environ.get("DAGTOOLS_UPSTREAM_EV_CONTRACT")
    if override:
        candidate = Path(override)
        return candidate if candidate.is_file() else None
    sibling = REPO.parent / "invincible-agent" / "contracts" / "ev_fixture.yaml"
    return sibling if sibling.is_file() else None


def test_the_vendored_contract_has_not_drifted_from_upstream():
    """Byte-for-byte, which is why the vendored copy carries no added header.

    Provenance lives in the example's README instead. A prepended comment here
    would make every comparison a diff and this canary useless.
    """
    upstream = _upstream_contract()
    if upstream is None:
        pytest.skip(
            "the upstream contract is not on this machine; set "
            "DAGTOOLS_UPSTREAM_EV_CONTRACT to a path to compare"
        )
    assert CONTRACT_FILE.read_bytes() == upstream.read_bytes()

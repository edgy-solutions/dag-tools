"""The refusal fixture, checked against the contract it is planted from.

Two things are worth proving about a fixture whose whole purpose is to be
rejected, and neither is "the asset materializes".

**That every rule has a case.** The contract's ``rules:`` block is the list of
ids a reader's 502 can name. It is read here out of the contract file, so a
rule added there fails this fixture until a case is planted for it, rather
than leaving a refusal path with no exercise and nobody noticing.

**That each case trips its own rule and no other.** Every rule's check is run
against every case. A case that broke two rules would prove nothing -- a
reader refusing on the first would never reach the second -- and a case that
tripped a rule other than its own would make a reader that refuses for the
wrong reason look correct. The control scope trips nothing, which is what
separates a planted scope from an accidentally malformed asset.

The checks below are this file's own reading of the rules' wording. They are
deliberately NOT the reader's implementation and must not become it: dag-tools
ships the contract and the rows, and the engine refuses. What they establish
is that the rows say what the manifest claims they say.

Everything is asserted on the frames **after a parquet round trip**, because a
planted null and a planted NaN are exactly the two values an IO manager can
quietly turn into something else, and a fixture that only holds them in memory
would not have tested the path a reader reads on.

No test names a program. Nothing in this repository should.
"""
from __future__ import annotations

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("polars")
pytest.importorskip("pandas")   # arrow.py imports pandas at module level
pytest.importorskip("s3fs")     # ...and s3fs
yaml = pytest.importorskip("yaml")

from pathlib import Path
from typing import Dict, Set

import polars as pl
from dagster import Definitions, materialize

from dag_tools.contract_mapping.contract import Contract, load_contract
from dag_tools.io_managers.arrow import ConfigurableArrowIOManager, LocalFSConfig
from dag_tools.user_deployment.demo_ev_refusal import (
    CLEAN_SCOPE,
    EV_REFUSAL_CSV,
    IO_MANAGER_KEY,
    MISSING_COLUMN_ASSET,
    MISSING_COLUMN_PLANTED,
    REFUSAL_ASSETS,
    REFUSAL_CASES,
    ROW_LEVEL_ASSET,
    SCOPE_COLUMN,
    WRONG_TYPE_ASSET,
    build_refusal_defs,
    case_for,
    refusal_scope,
    scopes_of,
)
from dag_tools.user_deployment.mesh_demo_assets import (
    EV_FIXTURE_CSV,
    build_demo_defs,
    build_mesh_demo_io_manager,
)

# The contract is the source of record for which rule ids exist. It lives in
# `examples/`, which is part of the contract surface rather than a sample: the
# fixture is planted from this file, so reading it here is what keeps the two
# from drifting.
CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "contract_mapping"
    / "contracts"
    / "ev_fixture.yaml"
)

# Which rule covers a broken `constant_within`, by the column that broke it.
# Four of the contract's rules are stated over the one mechanism -- an amount
# that must not vary inside something -- and three of them name their column in
# their own wording while the fourth is the general case. That split is this
# file's reading of the wording, so it is checked against the contract in
# `test_the_constant_within_rules_name_contract_columns_and_contract_rules`.
CONSTANT_WITHIN_RULES = {
    "program_name": "program_name_not_unique",
    "program_bac": "program_bac_not_unique",
    "control_account": "work_package_in_two_accounts",
}
GENERAL_CONSTANT_WITHIN_RULE = "varies_within_path"

TYPE_CHECKS = {
    "string": lambda dtype: dtype == pl.String,
    "numeric": lambda dtype: dtype.is_numeric(),
    "integer": lambda dtype: dtype.is_integer(),
    "float": lambda dtype: dtype.is_float(),
}


@pytest.fixture(scope="module")
def contract() -> Contract:
    return load_contract(yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def calendar_years() -> Set[str]:
    """The fiscal years the calendar covers, taken from the accepted fixture.

    The contract names its calendar (`period.vocabulary`) and does not carry
    it, and `contract.py` is explicit that a name cannot be tested against. So
    the only plant that is outside the calendar whatever its months turn out to
    be is one that leaves the fiscal year the accepted fixture reports in --
    and that is the boundary this derives.
    """
    periods = pl.read_csv(EV_FIXTURE_CSV, schema_overrides={"period": pl.String})
    return {value.split("-")[0] for value in periods["period"].to_list()}


@pytest.fixture(scope="module")
def frames(tmp_path_factory) -> Dict[str, pl.DataFrame]:
    """Each refusal asset as a reader gets it: materialized, then read back.

    One root per asset so the parquet a test wants is the only one under it;
    the point of going through the real manager at all is the round trip.
    """
    built: Dict[str, pl.DataFrame] = {}
    for definition in REFUSAL_ASSETS:
        name = definition.key.to_user_string()
        root = tmp_path_factory.mktemp(name)
        manager = ConfigurableArrowIOManager(fs=LocalFSConfig(), uri_base=str(root))
        assert materialize([definition], resources={IO_MANAGER_KEY: manager}).success
        written = list(root.rglob("*.parquet"))
        assert written, f"{name} produced no parquet"
        built[name] = pl.read_parquet(written)
    return built


# ---------------------------------------------------------------------------
# The checks. One per rule id, read off the rules' own wording, run against
# whatever frame a case names.
# ---------------------------------------------------------------------------


def _tripped(frame: pl.DataFrame, contract: Contract, calendar_years: Set[str]) -> Set[str]:
    """Every rule id these rows break."""
    broken: Set[str] = set()
    present = set(frame.columns)

    if {column.name for column in contract.must_produce} - present:
        broken.add("missing_column")

    for column in contract.columns:
        if column.name not in present:
            continue
        check = TYPE_CHECKS.get(column.type)
        if check is not None and not check(frame.schema[column.name]):
            broken.add("wrong_type")

        series = frame[column.name]
        if series.null_count():
            broken.add("null_value")
        if series.dtype.is_float():
            # `is_nan` answers null for a null, so the two rules stay distinct:
            # a reader that let a null answer here would refuse the null case
            # under the non-finite id.
            nan = bool(series.is_nan().fill_null(False).any())
            infinite = bool(series.is_infinite().fill_null(False).any())
            if nan or infinite:
                broken.add("non_finite")

        if column.constant_within and set(column.constant_within) <= present:
            varies = (
                frame.group_by(column.constant_within)
                .agg(pl.col(column.name).n_unique().alias("values"))["values"]
                .max()
            )
            if varies and varies > 1:
                broken.add(
                    CONSTANT_WITHIN_RULES.get(
                        column.name, GENERAL_CONSTANT_WITHIN_RULE
                    )
                )

    if contract.scope in present and frame[contract.scope].n_unique() > 1:
        broken.add("other_program_rows")

    if contract.grain and set(contract.grain) <= present:
        if bool(frame.select(contract.grain).is_duplicated().any()):
            broken.add("duplicate_work_package_period")

    period = contract.period.column
    if period in present:
        years = {value.split("-")[0] for value in frame[period].to_list()}
        if years - calendar_years:
            broken.add("period_outside_calendar")

    return broken


def _rows_of(frames, case) -> pl.DataFrame:
    frame = frames[case.asset]
    if case.scope is None:
        return frame
    return frame.filter(pl.col(SCOPE_COLUMN) == case.scope)


SCOPED_CASES = [case for case in REFUSAL_CASES if case.scope is not None]


# ---------------------------------------------------------------------------
# The fixture covers the contract, and the contract covers the fixture.
# ---------------------------------------------------------------------------


def test_every_contract_rule_has_a_planted_case(contract):
    """The load-bearing one: a rule added to the contract fails here.

    Without it, a new refusal path reaches a reader with nothing to exercise
    it, and the fixture keeps reporting full coverage of a shorter list.
    """
    uncovered = set(contract.rules) - {case.rule for case in REFUSAL_CASES}
    assert not uncovered, f"contract rules with no planted case: {sorted(uncovered)}"


def test_no_case_names_a_rule_the_contract_does_not_have(contract):
    """The other direction: a case for a rule that no longer exists is dead
    rows that look like coverage."""
    unknown = {case.rule for case in REFUSAL_CASES} - set(contract.rules)
    assert not unknown, f"planted cases for unknown rules: {sorted(unknown)}"


def test_one_case_per_rule():
    rules = [case.rule for case in REFUSAL_CASES]
    assert len(rules) == len(set(rules)), "a rule is planted twice"


def test_every_scope_addresses_one_case():
    scopes = [case.scope for case in SCOPED_CASES]
    assert len(scopes) == len(set(scopes)), "two cases share a scope"
    assert CLEAN_SCOPE not in scopes, "the control scope is also a case"


def test_the_control_is_not_a_rule(contract):
    """`NP-PLANT-NONE` is derived the same way a case's scope is, so a rule
    named `none` would silently turn the control into a case and the fixture
    would lose the thing that distinguishes a plant from a broken asset."""
    assert "none" not in contract.rules
    assert CLEAN_SCOPE == refusal_scope("none")


def test_the_constant_within_rules_name_contract_columns_and_contract_rules(contract):
    """The one piece of judgment in this file, checked against the document."""
    for column, rule in CONSTANT_WITHIN_RULES.items():
        assert contract.column(column) is not None, f"{column} is not a contract column"
        assert rule in contract.rules, f"{rule} is not a contract rule"
    assert GENERAL_CONSTANT_WITHIN_RULE in contract.rules


# ---------------------------------------------------------------------------
# Each case trips its own rule, and only its own rule.
# ---------------------------------------------------------------------------


def test_the_control_scope_trips_nothing(frames, contract, calendar_years):
    """The control for every test below. If this fails, a refusal anywhere in
    this asset says nothing about the plant it was meant to demonstrate."""
    rows = frames[ROW_LEVEL_ASSET].filter(pl.col(SCOPE_COLUMN) == CLEAN_SCOPE)
    assert rows.height > 0, "the control scope produced no rows"
    assert _tripped(rows, contract, calendar_years) == set()


@pytest.mark.parametrize("case", SCOPED_CASES, ids=lambda case: case.rule)
def test_a_case_trips_exactly_its_own_rule(case, frames, contract, calendar_years):
    rows = _rows_of(frames, case)
    assert rows.height > 0, f"{case.scope} produced no rows"
    assert _tripped(rows, contract, calendar_years) == {case.rule}, case.planted


def test_the_unscoped_case_is_the_asset_holding_every_scope(frames, contract, calendar_years):
    """`other_program_rows` is the one rule with no planted defect: the
    contract says the asset may hold other scopes' rows and that a read which
    kept them forgot its filter. So the whole asset trips it and no single
    scope does -- which is also what makes every other case addressable."""
    case = case_for("other_program_rows")
    assert case is not None and case.scope is None

    whole = frames[ROW_LEVEL_ASSET]
    assert "other_program_rows" in _tripped(whole, contract, calendar_years)
    assert whole[SCOPE_COLUMN].n_unique() > 1

    for scope in scopes_of(ROW_LEVEL_ASSET):
        rows = whole.filter(pl.col(SCOPE_COLUMN) == scope)
        assert "other_program_rows" not in _tripped(rows, contract, calendar_years)


# ---------------------------------------------------------------------------
# The two plants a scope filter cannot reach.
# ---------------------------------------------------------------------------


def test_the_missing_column_asset_is_short_exactly_one_required_column(frames, contract):
    frame = frames[MISSING_COLUMN_ASSET]
    absent = {column.name for column in contract.must_produce} - set(frame.columns)
    assert absent == {MISSING_COLUMN_PLANTED}


def test_the_missing_column_is_not_the_scope_column(contract):
    """Withholding the scope would leave a reader unable to filter to the case,
    so the refusal could never be addressed -- the plant would be unreachable
    rather than planted."""
    assert MISSING_COLUMN_PLANTED != contract.scope
    assert MISSING_COLUMN_PLANTED not in contract.grain


def test_the_wrong_type_asset_disagrees_with_the_contract_on_one_column(frames, contract):
    frame = frames[WRONG_TYPE_ASSET]
    wrong = [
        column.name
        for column in contract.columns
        if column.name in frame.columns
        and column.type in TYPE_CHECKS
        and not TYPE_CHECKS[column.type](frame.schema[column.name])
    ]
    assert len(wrong) == 1
    assert contract.column(wrong[0]).type == "string"
    assert frame.schema[wrong[0]].is_numeric(), (
        "the plant is the type a producer gets by inference, so it has to be "
        "the inferred one rather than any non-string"
    )


# ---------------------------------------------------------------------------
# What the round trip is for: the two values a parquet write could change.
# ---------------------------------------------------------------------------


def test_the_planted_null_survives_the_write_and_is_not_a_nan(frames):
    rows = frames[ROW_LEVEL_ASSET].filter(
        pl.col(SCOPE_COLUMN) == refusal_scope("null_value")
    )
    nulls = [name for name in rows.columns if rows[name].null_count()]
    assert len(nulls) == 1, f"expected one column to hold the planted null, got {nulls}"
    series = rows[nulls[0]]
    assert series.dtype.is_float()
    assert not bool(series.is_nan().fill_null(False).any()), (
        "the planted null came back as a NaN, which is a different rule"
    )


def test_the_planted_nan_survives_the_write_and_is_not_a_null(frames):
    rows = frames[ROW_LEVEL_ASSET].filter(
        pl.col(SCOPE_COLUMN) == refusal_scope("non_finite")
    )
    nan = [
        name
        for name in rows.columns
        if rows[name].dtype.is_float()
        and bool(rows[name].is_nan().fill_null(False).any())
    ]
    assert len(nan) == 1, f"expected one column to hold the planted NaN, got {nan}"
    assert rows.null_count().sum_horizontal().item() == 0, (
        "the planted NaN came back as a null, which is a different rule"
    )


def test_the_duplicate_rows_are_identical_in_every_column(frames, contract):
    """What a rewrite that left an old part file produces, and the reason that
    case trips nothing else: two rows differing anywhere else would also break
    whichever rule holds over the column they differed in."""
    rows = frames[ROW_LEVEL_ASSET].filter(
        pl.col(SCOPE_COLUMN) == refusal_scope("duplicate_work_package_period")
    )
    duplicated = rows.filter(rows.select(contract.grain).is_duplicated())
    assert duplicated.height == 2
    assert duplicated.unique().height == 1


def test_the_planted_period_leaves_the_calendars_fiscal_year(frames, contract, calendar_years):
    rows = frames[ROW_LEVEL_ASSET].filter(
        pl.col(SCOPE_COLUMN) == refusal_scope("period_outside_calendar")
    )
    years = {value.split("-")[0] for value in rows[contract.period.column].to_list()}
    assert years - calendar_years, (
        "every planted period is in the calendar's fiscal year, so a reader "
        "with any calendar at all would accept them"
    )


# ---------------------------------------------------------------------------
# Where it is wired, and where it must not be.
# ---------------------------------------------------------------------------


def test_the_refusal_definitions_carry_every_case(frames):
    keys = {
        key.to_user_string()
        for key in build_refusal_defs().resolve_asset_graph().get_all_asset_keys()
    }
    assert keys == {case.asset for case in REFUSAL_CASES}


def test_it_writes_through_the_same_manager_as_the_accepted_fixture():
    """Same configuration, own resource key. A refusal fixture on a different
    manager would be testing a writer no accepted read uses."""
    refusal = build_refusal_defs().resources[IO_MANAGER_KEY]
    assert isinstance(refusal, ConfigurableArrowIOManager)
    assert refusal.uri_base == build_mesh_demo_io_manager().uri_base


def test_it_merges_with_the_demo_surface(frames):
    """`Definitions.merge` raises on a conflicting resource key, which is why
    this surface has its own -- and this is the call that would have found it."""
    merged = Definitions.merge(build_demo_defs(), build_refusal_defs())
    keys = {key.to_user_string() for key in merged.resolve_asset_graph().get_all_asset_keys()}
    assert {case.asset for case in REFUSAL_CASES} <= keys
    assert "ev_fixture" in keys


def test_a_deployment_with_demo_mode_off_does_not_have_them(monkeypatch):
    """Rows planted to be refused are the last thing a production deployment
    should carry, so the gate is asserted rather than assumed."""
    from dag_tools.user_deployment import definitions

    monkeypatch.delenv("DAG_TOOLS_GRIST_CONFIG", raising=False)
    monkeypatch.delenv("DATAHUB_SERVER", raising=False)

    def keys() -> Set[str]:
        graph = definitions._build_combined_defs().resolve_asset_graph()
        return {key.to_user_string() for key in graph.get_all_asset_keys()}

    monkeypatch.setenv("DAG_TOOLS_DEMO_MODE", "true")
    assert ROW_LEVEL_ASSET in keys()  # the control: the same call, the switch on
    monkeypatch.delenv("DAG_TOOLS_DEMO_MODE")
    assert not {case.asset for case in REFUSAL_CASES} & keys()


def test_the_csv_ships_inside_the_package():
    """The wheel packages ``dag_tools`` alone, so the file must sit beneath it."""
    assert "dag_tools" in EV_REFUSAL_CSV.parts and EV_REFUSAL_CSV.is_file()

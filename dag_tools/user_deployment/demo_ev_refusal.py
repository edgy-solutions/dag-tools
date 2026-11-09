"""The refusal side of the synthetic earned-value fixture: one planted error
per contract rule.

``mesh_demo_assets.ev_fixture`` is the fixture a governed read is supposed to
*accept*. Nothing was the fixture a governed read is supposed to *refuse*, so
every refusal a reader declares existed only in the reader's own unit tests,
over frames it built itself. A refusal that has never travelled through an IO
manager, a parquet round trip and a scope filter is a refusal nobody has seen
fire, and the rules most worth exercising are exactly the ones a reader is
least likely to have got right.

These assets carry rows that are *meant* to be rejected. They are demo-gated
for the same reason the rest of this surface is -- production must not grow
synthetic tables -- and every program they name begins ``NP-PLANT-`` so a row
that escapes into a catalog says what it is.

**One case per rule, and one rule per case.** A frame carrying two violations
proves nothing: a reader refusing on the first would never reach the second,
and a reader refusing for the *wrong* rule would look correct. So each case is
planted in isolation, and ``test_demo_ev_refusal.py`` runs every rule's check
against every case and asserts that exactly the planted one trips.

**A case is addressed by scope, because that is how the contract addresses
anything.** A read filters to one value of the scope column and holds no
other, so one synthetic program per rule gives a reader an address for each
refusal: read ``NP-PLANT-NULL-VALUE`` and you meet that rule and nothing else.
``NP-PLANT-NONE`` is the control -- a program in the same asset that violates
nothing -- which is what separates "this scope is planted" from "this asset is
broken". Without it, an asset accidentally malformed as a whole would refuse
every scope and read as a full set of successful cases.

**Two rules cannot be planted in a scope, so each gets its own asset.** A
column's absence and a column's type are properties of the frame, not of a
row: no filter can reach them. Those two assets carry one program each, named
after the rule they plant, so the address is still the rule.

**One rule is not a defect at all.** The contract says the asset *may* hold
other scopes' rows, and that a read which kept them forgot its filter. That
rule is therefore planted by the row-level asset holding every case's program
at once -- its own shape, not a bad row -- and its case carries no scope.

The rule ids are the reader's, verbatim from the contract's ``rules:`` block,
so a failing test here and a reader's 502 name the same thing. The contract is
the source of record for which ids exist; this module is checked against it
rather than the other way round, and a rule added there fails this fixture
until a case is planted for it.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from dagster import Definitions, asset

from dag_tools.user_deployment.mesh_demo_assets import (
    EV_FIXTURE_FLOAT_COLUMNS,
    EV_FIXTURE_INTEGER_COLUMNS,
    EV_FIXTURE_STRING_COLUMNS,
    build_mesh_demo_io_manager,
)

EV_REFUSAL_CSV = Path(__file__).parent / "demo_data" / "ev_refusal_fixture.csv"

# The contract's ``scope``: the column a read filters to one value of, and so
# the column a case is addressed by. The values themselves are all in the CSV.
SCOPE_COLUMN = "program_id"

# The three asset keys. The row-level one carries every case a scope filter can
# reach; the other two exist only because their rule cannot be reached by one.
ROW_LEVEL_ASSET = "ev_refusal_fixture"
MISSING_COLUMN_ASSET = "ev_refusal_missing_column"
WRONG_TYPE_ASSET = "ev_refusal_wrong_type"

GROUP_NAME = "mesh_demo_refusal"

# The column ``ev_refusal_missing_column`` withholds. A required amount column,
# never the scope column: dropping the scope would leave a reader unable to
# filter to the case at all, so the refusal could never be addressed.
MISSING_COLUMN_PLANTED = "bcwp"

# The column ``ev_refusal_wrong_type`` leaves to inference. The contract
# declares it a string and remarks on exactly this hazard -- a control account
# such as ``1.1`` infers as a float -- so the planted defect is the one a
# producer actually makes rather than a contrived one.
WRONG_TYPE_PLANTED = "control_account"

# This surface's own resource key. ``Definitions.merge`` raises on conflicting
# resource keys, so the refusal assets cannot share the demo surface's key even
# though they want the same manager.
IO_MANAGER_KEY = "ev_refusal_io"


def refusal_scope(rule: str) -> str:
    """The scope value that addresses one rule's planted case.

    Derived rather than tabulated, so a reader, this module and the fixture's
    tests all spell it the same way.
    """
    return "NP-PLANT-" + rule.upper().replace("_", "-")


# The control. ``none`` is not a rule id, and a test asserts the contract never
# grows one -- a rule by that name would collide with the clean program, and the
# control would quietly become a case.
CLEAN_SCOPE = refusal_scope("none")


@dataclass(frozen=True)
class RefusalCase:
    """One rule, one planted error, and where to find it."""

    rule: str
    asset: str
    scope: Optional[str]
    planted: str


# The manifest, and the only place that says which case lives where: the assets
# derive their own row filters from it, so a case cannot be declared here and
# left out of the rows, nor produced in the rows and left undeclared.
REFUSAL_CASES: Tuple[RefusalCase, ...] = (
    RefusalCase(
        rule="missing_column",
        asset=MISSING_COLUMN_ASSET,
        scope=refusal_scope("missing_column"),
        planted=(
            "the asset does not produce a required amount column at all; a "
            "column's absence is a property of the frame, so no scope filter "
            "could reach it"
        ),
    ),
    RefusalCase(
        rule="wrong_type",
        asset=WRONG_TYPE_ASSET,
        scope=refusal_scope("wrong_type"),
        planted=(
            "a column the contract declares a string is produced as a float, "
            "by leaving it to inference the way a producer does"
        ),
    ),
    RefusalCase(
        rule="null_value",
        asset=ROW_LEVEL_ASSET,
        scope=refusal_scope("null_value"),
        planted="one row holds no value in an amount column that is read",
    ),
    RefusalCase(
        rule="non_finite",
        asset=ROW_LEVEL_ASSET,
        scope=refusal_scope("non_finite"),
        planted=(
            "one row holds a NaN in an amount column. A NaN, not a null: they "
            "are different rules, and a reader that conflates them refuses "
            "under the wrong id"
        ),
    ),
    RefusalCase(
        rule="other_program_rows",
        asset=ROW_LEVEL_ASSET,
        scope=None,
        planted=(
            "the asset holds every case's scope at once. This is not a defect "
            "in the rows -- the contract says the asset may hold them -- so the "
            "case has no scope of its own: what is refused is a read that came "
            "back with more than one"
        ),
    ),
    RefusalCase(
        rule="program_name_not_unique",
        asset=ROW_LEVEL_ASSET,
        scope=refusal_scope("program_name_not_unique"),
        planted="the scope's rows carry two names for the one program",
    ),
    RefusalCase(
        rule="program_bac_not_unique",
        asset=ROW_LEVEL_ASSET,
        scope=refusal_scope("program_bac_not_unique"),
        planted="the scope's rows carry two budgets for the one program",
    ),
    RefusalCase(
        rule="work_package_in_two_accounts",
        asset=ROW_LEVEL_ASSET,
        scope=refusal_scope("work_package_in_two_accounts"),
        planted="one work package is reported under two control accounts",
    ),
    RefusalCase(
        rule="duplicate_work_package_period",
        asset=ROW_LEVEL_ASSET,
        scope=refusal_scope("duplicate_work_package_period"),
        planted=(
            "one grain tuple has two rows, identical in every column -- which "
            "is what the rewrite that left an old part file produces, and which "
            "trips no other rule"
        ),
    ),
    RefusalCase(
        rule="period_outside_calendar",
        asset=ROW_LEVEL_ASSET,
        scope=refusal_scope("period_outside_calendar"),
        planted=(
            "one row reports a period from the next fiscal year. The contract "
            "names its calendar and does not carry it, so the only plant that "
            "is outside it whatever its months turn out to be is one that "
            "leaves its year"
        ),
    ),
    RefusalCase(
        rule="varies_within_path",
        asset=ROW_LEVEL_ASSET,
        scope=refusal_scope("varies_within_path"),
        planted=(
            "one work package's completion figure differs between its period "
            "rows, so the amount a reader takes once per path is not there to "
            "take"
        ),
    ),
)


def case_for(rule: str) -> Optional[RefusalCase]:
    """The case planted for one rule id, if there is one."""
    for case in REFUSAL_CASES:
        if case.rule == rule:
            return case
    return None


def scopes_of(asset_name: str) -> Tuple[str, ...]:
    """The scope values one refusal asset carries, derived from the manifest.

    The row-level asset also carries the control, which is what makes a refusal
    there attributable to a plant rather than to the asset.
    """
    scopes = {case.scope for case in REFUSAL_CASES if case.asset == asset_name}
    scopes.discard(None)
    if asset_name == ROW_LEVEL_ASSET:
        scopes.add(CLEAN_SCOPE)
    return tuple(sorted(scope for scope in scopes if scope is not None))


def _rows_for(asset_name: str, *, infer: Tuple[str, ...] = ()):
    """The CSV's rows for one asset, typed the way the clean fixture types them.

    The two fixtures share one column declaration, so a column added to the
    accepted fixture cannot be forgotten here. ``infer`` is how the wrong-type
    plant is made: the named columns are left to inference rather than declared.
    """
    # Lazy import -- see the note in ``mesh_demo_assets``.
    import polars as pl

    overrides = {
        **{column: pl.String for column in EV_FIXTURE_STRING_COLUMNS},
        **{column: pl.Int64 for column in EV_FIXTURE_INTEGER_COLUMNS},
        **{column: pl.Float64 for column in EV_FIXTURE_FLOAT_COLUMNS},
    }
    for column in infer:
        overrides.pop(column, None)

    frame = pl.read_csv(EV_REFUSAL_CSV, schema_overrides=overrides)
    return frame.filter(pl.col(SCOPE_COLUMN).is_in(list(scopes_of(asset_name))))


# ``context`` is left unannotated for the reason given in ``mesh_demo_assets``.
@asset(
    name=ROW_LEVEL_ASSET,
    group_name=GROUP_NAME,
    io_manager_key=IO_MANAGER_KEY,
    description=(
        "SYNTHETIC rows planted to be REFUSED: one case per contract rule a "
        "scope filter can reach, plus a clean control scope. Not a real "
        "program, and not rows any answer should be computed from."
    ),
)
def ev_refusal_fixture(context):
    """Materialize every planted case a read can address by scope."""
    frame = _rows_for(ROW_LEVEL_ASSET)
    context.log.info(
        "ev_refusal_fixture materialized: %d rows across %d scopes",
        frame.height,
        frame[SCOPE_COLUMN].n_unique(),
    )
    return frame


@asset(
    name=MISSING_COLUMN_ASSET,
    group_name=GROUP_NAME,
    io_manager_key=IO_MANAGER_KEY,
    description=(
        "SYNTHETIC rows planted to be REFUSED: a required column is absent. "
        "Its own asset because no scope filter can reach a missing column."
    ),
)
def ev_refusal_missing_column(context):
    """Materialize otherwise-clean rows with one required column withheld."""
    frame = _rows_for(MISSING_COLUMN_ASSET).drop(MISSING_COLUMN_PLANTED)
    context.log.info(
        "ev_refusal_missing_column materialized: %d rows, %d columns",
        frame.height,
        frame.width,
    )
    return frame


@asset(
    name=WRONG_TYPE_ASSET,
    group_name=GROUP_NAME,
    io_manager_key=IO_MANAGER_KEY,
    description=(
        "SYNTHETIC rows planted to be REFUSED: a column the contract declares "
        "a string arrives as a float. Its own asset because a column's type is "
        "a property of the frame, not of a row."
    ),
)
def ev_refusal_wrong_type(context):
    """Materialize otherwise-clean rows with one declared string left inferred."""
    frame = _rows_for(WRONG_TYPE_ASSET, infer=(WRONG_TYPE_PLANTED,))
    context.log.info(
        "ev_refusal_wrong_type materialized: %d rows, %s arrives as %s",
        frame.height,
        WRONG_TYPE_PLANTED,
        frame.schema[WRONG_TYPE_PLANTED],
    )
    return frame


REFUSAL_ASSETS = (
    ev_refusal_fixture,
    ev_refusal_missing_column,
    ev_refusal_wrong_type,
)


def build_refusal_defs() -> Definitions:
    """The refusal surface, on its own resource key.

    Same producer IO manager as the accepted fixture -- parquet, with
    coordinates the broker can advertise -- because a refusal that skipped the
    IO manager would not exercise the path a reader refuses on. The frames
    carry a null and a NaN, and whether those two survive a parquet round trip
    is part of what this fixture is for.
    """
    return Definitions(
        assets=list(REFUSAL_ASSETS),
        resources={IO_MANAGER_KEY: build_mesh_demo_io_manager()},
    )

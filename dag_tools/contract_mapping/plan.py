"""Check a mapping against its contract, and plan what to emit.

Everything that can be known before a row moves is decided here: whether the
mapping produces every column a governed read selects, whether its sources
exist in the source's schema, and which tests are worth writing. The output
is a plan -- data, renderable twice (SQL and rows) and hashable -- so the
component can show it in a dry run and the tests can assert on it without a
warehouse.

**Problems are collected, not raised one at a time.** A mapping is written by
hand against a contract of a dozen-odd columns, and the failure mode of
raising on the first problem is an operator fixing one name per code-location
reload. `MappingPlanError` carries every problem it found.

**The line between an error and a note is: would the emitted artifact be
wrong?** An unmapped required column makes a model a governed read refuses,
so it is an error. A contract naming a period vocabulary instead of listing
it means one test cannot be written -- the model is still correct, so it is a
note. Notes ride on the plan and into the materialization metadata; they are
never swallowed, because the one thing worse than an unwritten test is an
unwritten test nobody is told about.
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pydantic import BaseModel, ConfigDict, Field

from dag_tools.contract_mapping.contract import CONTRACT_TYPES, Contract
from dag_tools.contract_mapping.mapping import Mapping

# The rule ids this library reaches for, by the kind of test it emits.
#
# These are the CONTRACT GRAMMAR's own ids, not a customer's shape: a test
# emitted here and a governed read's refusal must name the same rule, or an
# operator holding a failing test cannot tell which read it protects. A
# contract is free to spell them differently -- every lookup is tolerant, the
# plan records whether the contract declared the id, and `mapping.rule_ids`
# overrides any of them outright.
RULE_IDS: Dict[str, str] = {
    "types_enforced": "wrong_type",
    "column_present": "missing_column",
    "not_null": "null_value",
    "non_finite": "non_finite",
    "period_in_calendar": "period_outside_calendar",
    "constant_within": "varies_within_path",
    "key_unique": "duplicate_grain",
}

#: Contract types over which a non-finite value is possible at all.
_NUMERIC_TYPES = ("numeric", "float")

#: Transforms whose SQL rendering does not carry the source column's type
#: through. `scale` renders as a multiplication, which most dialects widen to
#: a fractional type; `concat` produces text whatever it was given; a
#: `constant` of `null` is untyped. Under an enforced model contract each
#: needs a cast, and the plan says so when the mapping declares none.
_TYPE_CHANGING_TRANSFORMS = ("scale", "concat", "constant")


class MappingPlanError(Exception):
    """The mapping cannot be reconciled with its contract.

    Carries every problem found, so one reload fixes one mapping rather than
    one name.
    """

    def __init__(self, problems: Sequence[str]) -> None:
        self.problems = list(problems)
        joined = "\n".join(f"  - {problem}" for problem in self.problems)
        super().__init__(
            f"{len(self.problems)} problem(s) reconciling the mapping with its "
            f"contract:\n{joined}"
        )


class PlannedColumn(BaseModel):
    """One output column, with everything either renderer needs."""

    model_config = ConfigDict(extra="forbid")

    name: str
    contract_type: str
    warehouse_type: Optional[str] = None
    cast_template: Optional[str] = None

    transform: str = "copy"
    sources: List[str] = Field(default_factory=list)
    value: Any = None
    separator: str = ""
    factor: Optional[Decimal] = None
    default: Any = None

    required: bool = False
    unit: Optional[str] = None
    description: Optional[str] = None
    constant_within: List[str] = Field(default_factory=list)


class PlannedTest(BaseModel):
    """One test to emit.

    `declared_in_contract` is false when the contract does not list the rule
    id this test will fail under. The test is still emitted -- the invariant
    holds either way -- but the plan says so, because a failing test naming a
    rule the contract never stated leaves an operator asking "refused by
    what?".
    """

    model_config = ConfigDict(extra="forbid")

    rule_id: str
    kind: str
    columns: List[str] = Field(default_factory=list)
    within: List[str] = Field(default_factory=list)
    values: List[str] = Field(default_factory=list)
    predicate: Optional[str] = None
    declared_in_contract: bool = True


class MappingPlan(BaseModel):
    """What a mapping, reconciled with its contract, says to emit."""

    model_config = ConfigDict(extra="forbid")

    contract_id: str
    contract_version: Optional[str] = None
    model_name: str

    dataset_urn: Optional[str] = Field(
        default=None,
        description="The contract's asset of record -- the physical platform's URN.",
    )
    source_urn: Optional[str] = Field(
        default=None,
        description="The source dataset's physical URN, carried as lineage.",
    )
    source_table: Optional[str] = None

    grain: List[str] = Field(default_factory=list)
    scope: Optional[str] = None
    period_column: Optional[str] = None

    columns: List[PlannedColumn] = Field(default_factory=list)
    tests: List[PlannedTest] = Field(default_factory=list)

    unmapped_declared: List[str] = Field(
        default_factory=list,
        description=(
            "Contract columns nothing produces yet (`produced_since: null`) "
            "that the mapping leaves unmapped. Expected, and listed so the "
            "operator can see the contract's forward edge."
        ),
    )
    notes: List[str] = Field(
        default_factory=list,
        description="Everything that was not emitted, and why.",
    )

    @property
    def plan_hash(self) -> str:
        """A stable hash of what will be emitted.

        Over the columns and tests only -- not the notes, which can gain a
        sentence without the artifact changing. Lets a materialization say
        "same bytes as last time" without diffing files.
        """
        payload = json.dumps(
            {
                "contract_id": self.contract_id,
                "model_name": self.model_name,
                "columns": [c.model_dump(mode="json") for c in self.columns],
                "tests": [t.model_dump(mode="json") for t in self.tests],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]

    @property
    def output_column_names(self) -> List[str]:
        return [column.name for column in self.columns]


def _rule_id(
    kind: str,
    mapping: Mapping,
    contract: Contract,
    derived: Optional[str] = None,
) -> Tuple[str, bool]:
    """Resolve the rule id a test of this kind fails under.

    Order: the mapping's explicit override, then a derived id the contract
    actually declares, then this library's generic id. Returns the id and
    whether the contract declares it.
    """
    override = mapping.rule_ids.get(kind)
    if override:
        return override, override in contract.rules

    if derived and derived in contract.rules:
        return derived, True

    generic = RULE_IDS[kind]
    if generic in contract.rules:
        return generic, True

    # Nothing declared. Prefer the derived spelling when there is one -- it
    # is the more specific thing to put in front of an operator.
    return (derived or generic), False


def build_mapping_plan(
    contract: Contract,
    mapping: Mapping,
    source_schema: Optional[Dict[str, str]] = None,
) -> MappingPlan:
    """Reconcile a mapping with its contract and plan the emission.

    `source_schema` is the source's columns as the catalog reports them. When
    it is None the mapping's own `source.columns` is used; when neither
    carries a schema, the source-column checks cannot run and the plan says
    so -- an unknown source column then fails at materialization instead of
    at the code-location load.
    """
    problems: List[str] = []
    notes: List[str] = []

    # -- the mapping is pointed at this contract --------------------------
    if mapping.contract and mapping.contract != contract.contract_id:
        problems.append(
            f"the mapping claims contract {mapping.contract!r} but was loaded "
            f"with {contract.contract_id!r}"
        )
    elif not mapping.contract:
        notes.append(
            "the mapping declares no `contract`, so nothing checks that it was "
            "paired with the contract it was written against"
        )

    schema = source_schema if source_schema is not None else dict(mapping.source.columns)
    schema_known = bool(schema)
    if not schema_known:
        notes.append(
            "no source schema available at planning time, so source columns "
            "are unverified until materialization"
        )

    # -- every column the mapping names is a column the contract declares --
    for name in mapping.columns:
        if contract.column(name) is None:
            problems.append(
                f"the mapping produces {name!r}, which the contract does not "
                "declare; a column outside the contract is a column no "
                "governed read will select"
            )

    # -- every column a read selects is produced --------------------------
    for column in contract.must_produce:
        if column.name not in mapping.columns:
            problems.append(
                f"contract column {column.name!r} is required and produced "
                f"since {column.produced_since}, but the mapping does not "
                "produce it"
            )

    unmapped_declared = [
        column.name
        for column in contract.columns
        if column.declared_not_produced and column.name not in mapping.columns
    ]

    for column in contract.columns:
        if column.declared_not_produced and column.name in mapping.columns:
            notes.append(
                f"the mapping produces {column.name!r}, which the contract "
                "declares but records as not yet produced -- this mapping is "
                "the release that starts producing it, so `produced_since` "
                "wants setting on the contract"
            )

    # -- the structural columns are produced ------------------------------
    period_column = mapping.period.key or contract.period.column
    if (
        mapping.period.key
        and contract.period.column
        and mapping.period.key != contract.period.column
    ):
        notes.append(
            f"the mapping names {mapping.period.key!r} as the period column "
            f"while the contract names {contract.period.column!r}; the "
            "mapping's wins here"
        )

    for grain_column in contract.grain:
        if grain_column not in mapping.columns:
            problems.append(
                f"grain column {grain_column!r} is not produced; the "
                "key-uniqueness test is stated over exactly the grain, so a "
                "missing grain column leaves the key untestable"
            )

    for role, name in (("scope", contract.scope), ("period", period_column)):
        if name and name not in mapping.columns:
            problems.append(f"the contract's {role} column {name!r} is not produced")

    # -- cadence ----------------------------------------------------------
    if (
        mapping.cadence.write
        and contract.cadence.write
        and mapping.cadence.write != contract.cadence.write
    ):
        problems.append(
            f"the mapping writes {mapping.cadence.write!r} but the contract "
            f"declares {contract.cadence.write!r}; a reader expecting one and "
            "served the other sees stale rows or duplicates, not an error"
        )
    if (
        mapping.cadence.set_by
        and contract.cadence.set_by
        and mapping.cadence.set_by != contract.cadence.set_by
    ):
        notes.append(
            f"cadence set_by differs: mapping {mapping.cadence.set_by!r}, "
            f"contract {contract.cadence.set_by!r}"
        )

    # -- per column -------------------------------------------------------
    planned: List[PlannedColumn] = []
    for column in contract.columns:
        entry = mapping.columns.get(column.name)
        if entry is None:
            continue

        if column.type not in CONTRACT_TYPES:
            problems.append(
                f"contract column {column.name!r} declares type "
                f"{column.type!r}, which this library does not know "
                f"({', '.join(CONTRACT_TYPES)}); it cannot be rendered or tested"
            )

        for source in entry.sources:
            if schema_known and source not in schema:
                problems.append(
                    f"{column.name!r} reads source column {source!r}, which "
                    "the source's schema does not carry"
                )

        if entry.transform == "scale" and column.type not in _NUMERIC_TYPES + ("integer",):
            problems.append(
                f"{column.name!r} is scaled but the contract declares it "
                f"{column.type!r}; scaling a non-numeric column is a mapping "
                "mistake, not a cast"
            )

        planned.append(
            PlannedColumn(
                name=column.name,
                contract_type=column.type,
                warehouse_type=mapping.warehouse_types.get(column.type),
                cast_template=mapping.cast_templates.get(column.type),
                transform=entry.transform,
                sources=list(entry.sources),
                value=entry.value,
                separator=entry.separator,
                factor=entry.factor,
                default=entry.default,
                required=column.required,
                unit=column.unit,
                description=entry.description,
                constant_within=list(column.constant_within),
            )
        )

    if problems:
        raise MappingPlanError(problems)

    # -- tests ------------------------------------------------------------
    tests: List[PlannedTest] = []

    if mapping.warehouse_types:
        missing = sorted({c.contract_type for c in planned if c.warehouse_type is None})
        if missing:
            notes.append(
                "no warehouse type is declared for contract type(s) "
                f"{', '.join(missing)}, so no model contract is enforced and "
                "neither the missing-column nor the wrong-type rule is checked"
            )
        else:
            uncast = [
                column.name
                for column in planned
                if column.transform in _TYPE_CHANGING_TRANSFORMS
                and not column.cast_template
            ]
            if uncast:
                notes.append(
                    "the model contract is enforced and "
                    f"{', '.join(uncast)} use a transform whose SQL does not "
                    "carry the column's type through (a multiplication widens, "
                    "a concatenation produces text, a null constant is "
                    "untyped), with no `cast_templates` entry for their "
                    "contract type -- so the warehouse may refuse the model on "
                    "a type mismatch that has nothing to do with the data"
                )
            for kind in ("column_present", "types_enforced"):
                rule_id, declared = _rule_id(kind, mapping, contract)
                tests.append(
                    PlannedTest(
                        rule_id=rule_id,
                        kind=kind,
                        columns=[c.name for c in planned],
                        declared_in_contract=declared,
                    )
                )
    else:
        notes.append(
            "the mapping declares no `warehouse_types`, so the emitted model "
            "enforces no contract and neither the missing-column nor the "
            "wrong-type rule is checked by it"
        )

    not_null = [c.name for c in planned if c.required]
    if not_null:
        rule_id, declared = _rule_id("not_null", mapping, contract)
        tests.append(
            PlannedTest(
                rule_id=rule_id,
                kind="not_null",
                columns=not_null,
                declared_in_contract=declared,
            )
        )

    if contract.grain:
        derived = "duplicate_" + "_".join(contract.grain)
        rule_id, declared = _rule_id("key_unique", mapping, contract, derived=derived)
        tests.append(
            PlannedTest(
                rule_id=rule_id,
                kind="key_unique",
                columns=list(contract.grain),
                declared_in_contract=declared,
            )
        )
    else:
        notes.append(
            "the contract states no grain, so no key-uniqueness test is "
            "written -- there is no stated key to be unique over"
        )

    for column in planned:
        for within in column.constant_within:
            if within not in mapping.columns:
                notes.append(
                    f"{column.name!r} is declared constant within {within!r}, "
                    "which this mapping does not produce, so the invariant is "
                    "not testable here"
                )
                continue
            rule_id, declared = _rule_id(
                "constant_within",
                mapping,
                contract,
                derived=f"{column.name}_not_unique",
            )
            tests.append(
                PlannedTest(
                    rule_id=rule_id,
                    kind="constant_within",
                    columns=[column.name],
                    within=[within],
                    declared_in_contract=declared,
                )
            )

    calendar = list(mapping.period.values) or list(contract.period.values)
    if period_column and calendar:
        rule_id, declared = _rule_id("period_in_calendar", mapping, contract)
        tests.append(
            PlannedTest(
                rule_id=rule_id,
                kind="period_in_calendar",
                columns=[period_column],
                values=calendar,
                declared_in_contract=declared,
            )
        )
    elif period_column and contract.period.vocabulary:
        notes.append(
            f"the calendar is named ({contract.period.vocabulary!r}) but not "
            "listed, so no period-membership test is written; list its values "
            "under the mapping's `period.values` to get one. A test against a "
            "name would pass without checking anything."
        )
    elif period_column:
        notes.append(
            "no calendar values are available, so no period-membership test "
            "is written"
        )

    numeric = [c.name for c in planned if c.contract_type in _NUMERIC_TYPES]
    if numeric and mapping.non_finite_predicate:
        rule_id, declared = _rule_id("non_finite", mapping, contract)
        tests.append(
            PlannedTest(
                rule_id=rule_id,
                kind="non_finite",
                columns=numeric,
                predicate=mapping.non_finite_predicate,
                declared_in_contract=declared,
            )
        )
    elif numeric:
        notes.append(
            "no `non_finite_predicate` is declared, so no non-finite test is "
            f"written for {len(numeric)} column(s); every warehouse spells "
            "that predicate differently, so this library cannot supply one"
        )

    undeclared = sorted({t.rule_id for t in tests if not t.declared_in_contract})
    if undeclared:
        notes.append(
            "the contract declares no rule for "
            f"{', '.join(undeclared)}; those tests are emitted under ids the "
            "contract does not state, so a failure names a rule no reader "
            "refuses under"
        )

    return MappingPlan(
        contract_id=contract.contract_id,
        contract_version=contract.version,
        model_name=mapping.model_name or contract.contract_id,
        dataset_urn=contract.dataset.urn,
        source_urn=mapping.source.dataset_urn,
        source_table=mapping.source.table,
        grain=list(contract.grain),
        scope=contract.scope,
        period_column=period_column,
        columns=planned,
        tests=tests,
        unmapped_declared=unmapped_declared,
        notes=notes,
    )

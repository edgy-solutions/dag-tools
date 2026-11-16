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
    "key_unique": "duplicate_key",
}

#: Contract types over which a non-finite value is possible at all.
_NUMERIC_TYPES = ("numeric", "float")

#: Transforms whose SQL rendering does not carry the source column's type
#: through. `scale` renders as a multiplication, which most dialects widen to
#: a fractional type; `concat` produces text whatever it was given; a
#: `constant` of `null` is untyped. Under an enforced model contract each
#: needs a cast, and the plan says so when the mapping declares none.
_TYPE_CHANGING_TRANSFORMS = ("scale", "concat", "constant", "in_set")


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
    values: Optional[List[Any]] = None
    assume_utc: bool = False

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
                # assume_utc joins the payload only where it is set, so a plan
                # that never uses it hashes as it did before the field existed.
                "columns": [
                    {
                        k: v
                        for k, v in c.model_dump(mode="json").items()
                        if not (k == "assume_utc" and v is False)
                        and not (k == "values" and v is None)
                    }
                    for c in self.columns
                ],
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


# The type words a `nullif` sentinel can be checked against, and the Python
# type a sentinel must then be.
#
# These are `CONTRACT_TYPES` spellings -- the one type vocabulary this library
# owns, and the one `source: columns:` is written in throughout the shipped
# examples. A warehouse's own spelling (`varchar`, `bigint`, `nvarchar`) is
# deliberately NOT listed, and not because the list would be hard to write:
# this library declares no warehouse types and no cast templates on purpose,
# because a guessed one emits SQL that fails for a reason that has nothing to
# do with the data -- and a table of dialect spellings here would be exactly
# the thing that rule refuses to own. An unrecognised spelling leaves the
# sentinel unchecked and says so, which is the stance every other check
# against this block already takes.
_SENTINEL_TYPE_FAMILIES = {"integer": int, "string": str}


def _check_sentinel_type(name, entry, schema, schema_known, problems, notes) -> None:
    """A `nullif` sentinel is compared with the source's raw value, so its type
    must be the declared type of the source column.

    Measured on duckdb: `nullif(<integer column>, '0')` casts the text to an
    integer and matches, while the row route compares `0 == "0"` and does not.
    The two routes would disagree on exactly the rows the mapping exists for.
    """
    source = entry.sources[0]
    if not schema_known:
        notes.append(
            f"{name!r} uses nullif, and with no source schema its sentinel "
            f"{entry.value!r} cannot be checked against the type of "
            f"{source!r}; a type mismatch makes the two routes disagree"
        )
        return
    declared = schema.get(source)
    if declared is None:
        return  # reported as an unknown source column
    family = _SENTINEL_TYPE_FAMILIES.get(declared.strip().lower())
    if family is None:
        notes.append(
            f"{name!r} uses nullif over source column {source!r} of declared "
            f"type {declared!r}, which is not one of the type words this "
            f"library reads ({', '.join(sorted(_SENTINEL_TYPE_FAMILIES))}), so "
            "the sentinel's type is unchecked"
        )
        return
    if not isinstance(entry.value, family):
        problems.append(
            f"{name!r} uses nullif with sentinel {entry.value!r} "
            f"({type(entry.value).__name__}) over source column {source!r} "
            f"declared {declared!r}; the two routes would disagree on exactly "
            "the rows the mapping writes it for, because the warehouse coerces "
            "the literal and the row route compares the written form"
        )


def _check_default_type(name, entry, contract_type, problems) -> None:
    """A `coalesce` fallback has to be a value the contract's column can take.

    `default` is `Any`, and the two routes do different things with one the
    column cannot carry: the row route hands it to `coerce_value`, which
    refuses it by name at materialization, while the SQL route emits it as a
    bare literal the warehouse either refuses or silently coerces -- and
    which of those it does is not this library's to decide, the same reason
    it declares no warehouse types. Knowable at the load, so refused there.

    The type read here is the CONTRACT's, which is this library's own
    vocabulary; no warehouse spelling is involved and none is listed.
    """
    # Deferred: `emit` imports this module, so the dependency only runs one
    # way at import time. `coerce_value` is the row route's own acceptance
    # test, which is the point -- a second copy of it here would be the
    # divergence this check exists to prevent.
    from dag_tools.contract_mapping.emit import CoercionError, coerce_value

    try:
        coerce_value(entry.default, contract_type, f"column {name!r}", entry.assume_utc)
    except CoercionError as error:
        problems.append(
            f"{name!r} coalesces to default {entry.default!r} "
            f"({type(entry.default).__name__}), which the contract's "
            f"{contract_type!r} column cannot take ({error}); the row route "
            "refuses it and the SQL route emits it as a bare literal the "
            "warehouse either refuses or coerces, so the two routes would "
            "disagree at the warehouse"
        )


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

        if entry.assume_utc and column.type != "timestamp":
            problems.append(
                f"{column.name!r} sets assume_utc but the contract declares it "
                f"{column.type!r}; the knob only means something on a "
                "timestamp, and a knob that says nothing must not look wired up"
            )

        if entry.transform == "in_set" and column.type != "boolean":
            problems.append(
                f"{column.name!r} is produced by in_set but the contract "
                f"declares it {column.type!r}; a membership test produces a "
                "boolean, and coercion would otherwise paper over the mistake "
                "by stringifying True"
            )

        if entry.transform == "nullif":
            _check_sentinel_type(
                column.name, entry, schema, schema_known, problems, notes
            )

        if (
            entry.transform == "coalesce"
            and entry.default is not None
            and column.type in CONTRACT_TYPES
        ):
            _check_default_type(column.name, entry, column.type, problems)

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
                values=entry.values,
                assume_utc=entry.assume_utc,
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
        # ONE id, never a spelling derived from the grain columns. The
        # derivation that used to stand here reproduced whatever the contract
        # happened to call its key rule, which meant the id a failing test
        # named changed with the grain -- two contracts over the same data
        # could not be read against each other, and a reader who learned the
        # rule once had to re-learn it per table. A contract whose own
        # vocabulary spells it differently says so in the mapping's
        # `rule_ids`, which is what that override is for.
        rule_id, declared = _rule_id("key_unique", mapping, contract)
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

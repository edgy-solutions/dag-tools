"""The mapping, read as data: this source's columns onto that contract's.

A mapping is the deployment's file. It names a source dataset, the contract
it claims to satisfy, and one entry per contract column saying where that
column comes from and what happens to it on the way. Nothing in this module
names a column, a table or a dialect; every value arrives from the document.

**Why a declared transform vocabulary rather than an expression.** The same
mapping has to render twice -- into SQL for the dbt model, and into an
operation applied to rows for the asset that cannot be a dbt model (a
file-backed asset has no warehouse to run SELECT in). An arbitrary SQL
expression renders once. So transforms are a small closed set, each with
both renderings, and a mapping that needs something outside the set is a
mapping that should name a view the warehouse already computes.

**Dialect stays out of the library.** `warehouse_types` and
`non_finite_predicate` are the mapping's, for the same reason step 2's tagged
warehouse ships no session-tag statement: every warehouse spells these
differently, and a default would be a name from below that is wrong somewhere.
Where a mapping declares neither, the plan emits a note saying which test was
not written -- never a test that passes without checking anything.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

#: What a transform does, and what it needs. The plan validates arguments
#: against this table so an unknown transform, or a known one missing its
#: argument, fails the component load rather than rendering a broken model.
TRANSFORMS: Dict[str, Dict[str, Any]] = {
    # name:        sources,          required args
    "copy": {"sources": 1, "requires": ()},
    "constant": {"sources": 0, "requires": ("value",)},
    "trim": {"sources": 1, "requires": ()},
    "upper": {"sources": 1, "requires": ()},
    "lower": {"sources": 1, "requires": ()},
    "concat": {"sources": "2+", "requires": ()},
    "coalesce": {"sources": "2+", "requires": ()},
    "scale": {"sources": 1, "requires": ("factor",)},
    "nullif": {"sources": 1, "requires": ("value",)},
    "in_set": {"sources": 1, "requires": ("values",)},
    "normalize": {"sources": 1, "requires": ("rules",)},
}


#: The six row-filter operators. One grammar for a mapping's `where` and the
#: dlt component's `row_filters`, which calls `validate_where` below.
WHERE_OPS = ("=", "!=", "in", "not_in", "is_null", "not_null")
_WHERE_SCALAR = ("=", "!=")
_WHERE_LIST = ("in", "not_in")


def _check_literals(literals: List[Any], who: str, many: bool) -> None:
    """The literal rules `in_set` and `where` share, written once.

    Text or integer only, one type per list: a bool is an int to Python and
    a flag to no warehouse, a float is an equality on floats, and a null is
    never matched by SQL `=`/`in` while Python matches it, so the routes
    would disagree on exactly the rows the mapping is written for. Binding a
    value instead of inlining it changes none of this, which is why the dlt
    component's `row_filters` is held to the same rules.
    """
    if many and not literals:
        raise ValueError(f"{who} requires a non-empty `values`")
    for v in literals:
        if isinstance(v, bool) or not isinstance(v, (str, int)):
            raise ValueError(
                f"{who} takes text or integer literals only, got "
                f"{literals!r}; a null is never matched by SQL `in` while "
                "Python matches it, so the routes would disagree, and a "
                "boolean or float is not a literal both routes compare alike "
                "(null is spelled is_null / not_null)"
            )
    if len({isinstance(v, str) for v in literals}) > 1:
        raise ValueError(
            f"{who} takes all text or all integer literals, "
            f"got a mix in {literals!r}; a dialect comparing text to an "
            "integer either errors or coerces, and which is not this "
            "library's to decide"
        )


def validate_where(flt: Dict[str, Any]) -> None:
    """Refuse a malformed row filter; the one validator both halves call.

    `flt` is one filter as written: only the keys the author wrote, so a
    present-but-null `value` is told apart from an absent one. Raises
    `ValueError` naming the filter. Checks the operator, which of `value` /
    `values` it takes and that the other is absent, and the literals. It
    does not check the column against a schema: that is the caller's, since
    only the caller knows where the schema comes from.
    """
    column = flt.get("column")
    op = flt.get("op")
    if not isinstance(column, str) or not column.strip():
        raise ValueError("a filter names no column")
    who = f"filter on {column!r} (op {op!r})"
    if op not in WHERE_OPS:
        raise ValueError(
            f"{who}: unknown operator; the operators are {', '.join(WHERE_OPS)}"
        )
    has_value, has_values = "value" in flt, "values" in flt
    if op in _WHERE_SCALAR:
        if not has_value or has_values:
            raise ValueError(f"{who} takes `value` and no `values`")
        _check_literals([flt["value"]], f"{who} value", False)
    elif op in _WHERE_LIST:
        if not has_values or has_value or flt["values"] is None:
            raise ValueError(f"{who} takes `values` and no `value`")
        if not isinstance(flt["values"], list) or not flt["values"]:
            raise ValueError(f"{who} has an empty `values` list; it needs a non-empty list")
        _check_literals(flt["values"], f"{who} values", True)
    elif has_value or has_values:
        raise ValueError(f"{who} takes neither `value` nor `values`")


class ColumnMapping(BaseModel):
    """Where one contract column comes from.

    `from` is spelled as it reads in YAML and is normalized to a list, so a
    single source and a pair of sources are the same shape downstream. It is
    absent only for `constant`, which is how a contract column no source
    carries is produced -- a status period stamped by the pipeline, say.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    sources: List[str] = Field(
        default_factory=list,
        alias="from",
        description="Source column name, or a list of them for concat/coalesce.",
    )
    transform: str = Field(
        default="copy",
        description=f"One of {', '.join(sorted(TRANSFORMS))}.",
    )

    value: Any = Field(default=None, description="`constant`: the literal to emit.")
    separator: str = Field(default="", description="`concat`: placed between sources.")
    factor: Optional[Decimal] = Field(
        default=None,
        description=(
            "`scale`: multiplied in. Decimal, not float -- a unit conversion "
            "of money through a binary float is how a total stops reconciling."
        ),
    )
    default: Any = Field(
        default=None,
        description=(
            "`coalesce`: used when every source is null. Optional, and it "
            "counts as one of coalesce's arguments -- one source with a "
            "declared fallback is a coalesce, a bare single source is not."
        ),
    )
    values: Optional[List[Any]] = Field(
        default=None,
        description=(
            "`in_set`: the literals a source value is tested against, all "
            "text or all integer. Matching is exact -- no case folding, no "
            "trimming -- because a source whose casing varies is a source "
            "whose enum is not an enum, and folding here would hide that. "
            "Not spelled `in`: that is a Python keyword, and `where` already "
            "names the source-block row filter."
        ),
    )

    rules: Optional[str] = Field(
        default=None,
        description=(
            "`normalize`: the id of a normalizer in the normalizer document. "
            "The mapping holds only the reference; every source that "
            "references one id is conformed identically."
        ),
    )
    part: Optional[str] = Field(
        default=None,
        description=(
            "`normalize`: which named group of the normalizer's "
            "`split_revision` pattern this column takes. The names are the "
            "operator's, so this is not a closed pair."
        ),
    )

    description: Optional[str] = Field(
        default=None,
        description="Carried onto the emitted model's column documentation.",
    )
    assume_utc: bool = Field(
        default=False,
        description=(
            "`timestamp` columns only: read a value that names no offset as "
            "UTC. A declared decision, never a default -- a naive value names "
            "no instant, and reading it as UTC shifts a non-UTC source by "
            "hours. Refused at plan time on any other contract type."
        ),
    )

    @field_validator("sources", mode="before")
    @classmethod
    def _one_or_many(cls, value: Union[str, List[str], None]) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return list(value)

    @model_validator(mode="after")
    def _check_transform(self) -> "ColumnMapping":
        spec = TRANSFORMS.get(self.transform)
        if spec is None:
            raise ValueError(
                f"unknown transform {self.transform!r}; this library renders "
                f"only {', '.join(sorted(TRANSFORMS))}. Anything else belongs "
                "in a view the warehouse already computes, named as the source."
            )

        wanted = spec["sources"]
        count = len(self.sources)
        if wanted == 0 and count:
            raise ValueError(
                f"transform {self.transform!r} takes no `from`, got {count}"
            )
        if wanted == 1 and count != 1:
            raise ValueError(
                f"transform {self.transform!r} takes exactly one `from`, got {count}"
            )
        # `coalesce` counts ARGUMENTS, and a declared `default` is one of
        # them: one source plus a constant fallback is two arguments, and
        # both routes already render that shape and agree on it. `concat`
        # is not relaxed alongside it -- it has no fallback to count, and
        # `concat(x)` with a separator is a genuine no-op.
        fallback = 1 if self.transform == "coalesce" and self.default is not None else 0
        if wanted == "2+" and count + fallback < 2:
            if self.transform == "coalesce":
                # The arity `coalesce` wants is two ARGUMENTS, and `default`
                # is one of them. Saying "add another `from`" here is what
                # sent operators looking for a second source they do not
                # have, and got a shipped, working feature written up as an
                # absent one.
                raise ValueError(
                    "transform 'coalesce' takes two or more arguments and got "
                    f"{count} `from` and "
                    f"{'a' if fallback else 'no'} `default`; a constant fallback "
                    "declared as `default` is the second argument, and is how "
                    "a one-source coalesce is written. Over a single source "
                    "with no fallback it is a no-op dressed as a transform."
                )
            raise ValueError(
                f"transform {self.transform!r} takes two or more `from`, got {count}"
            )

        for argument in spec["requires"]:
            if getattr(self, argument, None) is None:
                raise ValueError(
                    f"transform {self.transform!r} requires `{argument}`"
                )

        if self.transform == "nullif":
            # bool is an int in Python (`0 == False`) and not in a warehouse;
            # a float sentinel is an equality on floats.
            if isinstance(self.value, bool) or not isinstance(self.value, (str, int)):
                raise ValueError(
                    "transform 'nullif' takes a text or integer `value`, got "
                    f"{self.value!r}; a boolean would match an integer zero on "
                    "the row route and not in the warehouse, and a float is an "
                    "equality on floats"
                )
        if self.transform != "normalize":
            for knob in ("rules", "part"):
                if getattr(self, knob) is not None:
                    raise ValueError(
                        f"`{knob}` is set on transform {self.transform!r}; it "
                        "only means something on `normalize`, and a knob that "
                        "says nothing must not look wired up"
                    )
        if self.transform == "in_set":
            _check_literals(
                self.values or [], "transform 'in_set'", many=True
            )
        return self


class RowFilter(BaseModel):
    """One predicate on the source rows a mapping reads; see `validate_where`."""

    model_config = ConfigDict(extra="forbid")

    column: str
    op: str
    value: Optional[Any] = None
    values: Optional[List[Any]] = None

    @model_validator(mode="after")
    def _shape(self) -> "RowFilter":
        validate_where(
            {k: getattr(self, k) for k in self.model_fields_set} | {"column": self.column, "op": self.op}
        )
        return self


class SourceDataset(BaseModel):
    """The dataset the mapping reads from."""

    model_config = ConfigDict(extra="forbid")

    dataset_urn: Optional[str] = Field(
        default=None,
        description=(
            "The source's PHYSICAL platform dataset URN. Carried into the "
            "emitted catalog entry as lineage, so the mapped asset points at "
            "where its rows came from rather than at a name."
        ),
    )
    table: Optional[str] = Field(
        default=None,
        description=(
            "How the source is addressed in a FROM clause, as this warehouse "
            "spells it. Required for the dbt rendering; a rows-mode mapping "
            "needs no table."
        ),
    )
    columns: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "The source's schema, column -> type, when the mapping carries it. "
            "Leave empty to take it from the catalog at materialization; the "
            "cost is that an unknown source column then fails there rather "
            "than at the code location load."
        ),
    )
    where: List[RowFilter] = Field(
        default_factory=list,
        description=(
            "Which source rows the mapping reads: `[{column, op, value|values}]`, "
            "ANDed in the order written. A disjunction belongs in a view the "
            "mapping reads from. An excluded row produces no output row."
        ),
    )

    @model_validator(mode="after")
    def _where_names_declared_columns(self) -> "SourceDataset":
        # An empty `columns` means the schema comes from the catalog at
        # materialization; the plan notes the filter went unchecked then.
        if self.columns:
            for flt in self.where:
                if flt.column not in self.columns:
                    raise ValueError(
                        f"where filter names source column {flt.column!r}, "
                        f"which `source.columns` does not declare "
                        f"({sorted(self.columns)})"
                    )
        return self


class PeriodKey(BaseModel):
    """Which contract column is the period, and the calendar it is drawn from."""

    model_config = ConfigDict(extra="forbid")

    key: Optional[str] = Field(
        default=None,
        description=(
            "The contract column holding the period. Defaults to the "
            "contract's own `period.column`; set it only to disagree, which "
            "the plan reports."
        ),
    )
    values: List[str] = Field(
        default_factory=list,
        description=(
            "The calendar's values. Supplied here when the contract names a "
            "vocabulary instead of listing it -- a name cannot be tested "
            "against."
        ),
    )


class MappingCadence(BaseModel):
    """What this mapping's writes do, checked against the contract's."""

    model_config = ConfigDict(extra="forbid")

    write: Optional[str] = None
    set_by: Optional[str] = None


class Mapping(BaseModel):
    """A whole mapping document."""

    model_config = ConfigDict(extra="forbid")

    contract: Optional[str] = Field(
        default=None,
        description=(
            "The `contract_id` this mapping claims to satisfy. Checked against "
            "the contract it is loaded with, so a mapping pointed at the wrong "
            "contract fails the load instead of producing a plausible model."
        ),
    )
    source: SourceDataset = Field(default_factory=SourceDataset)
    period: PeriodKey = Field(default_factory=PeriodKey)
    cadence: MappingCadence = Field(default_factory=MappingCadence)

    columns: Dict[str, ColumnMapping] = Field(
        default_factory=dict,
        description="Contract column name -> where it comes from.",
    )

    warehouse_types: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Contract type -> this adapter's type name, for the emitted dbt "
            "model contract. Absent means no enforced data types are emitted "
            "and the plan says so."
        ),
    )
    cast_templates: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Contract type -> SQL that casts to it, with `{expr}` substituted. "
            "Needed because some transforms change a column's type class: "
            "`scale` on an integer column renders as a multiplication, which "
            "most dialects widen to a fractional type, and an enforced model "
            "contract then refuses the model for a reason that has nothing to "
            "do with the data. Where a type-changing transform has no cast, "
            "the plan says so rather than emitting SQL that fails at the "
            "warehouse."
        ),
    )
    regex_templates: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "`regex_replace` and `regex_extract`: SQL for a regex replace "
            "(`{expr}`, `{pattern}`, `{replacement}`) and a positional regex "
            "extract (`{expr}`, `{pattern}`, `{group}`), as this warehouse "
            "spells them. Operator-declared, like `cast_templates`, because "
            "this library names no dialect. Needed only by a normalizer "
            "using a regex op."
        ),
    )
    non_finite_predicate: Optional[str] = Field(
        default=None,
        description=(
            "SQL that is true for a non-finite value, with `{column}` "
            "substituted. Dialect-specific, so the mapping supplies it or no "
            "non-finite test is written."
        ),
    )

    rule_ids: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Test kind -> the rule id its failure should name, overriding the "
            "id this library would reach for. The escape hatch that keeps "
            "`plan.RULE_IDS` from being load-bearing: a contract written in a "
            "different rule vocabulary says so here instead of forcing a "
            "dag-tools release."
        ),
    )

    model_name: Optional[str] = Field(
        default=None,
        description="Name of the emitted dbt model. Defaults to the contract id.",
    )

    @field_validator("regex_templates")
    @classmethod
    def _templates_are_well_formed(cls, value: Dict[str, str]) -> Dict[str, str]:
        from dag_tools.contract_mapping.normalize import check_template

        for key, template in value.items():
            check_template(key, template)
        return value


def load_mapping(document: Dict[str, Any]) -> Mapping:
    """Validate a parsed mapping document.

    Dict in, model out, no IO -- same reason as `load_contract`. `columns`
    written as a list is the plausible mistake worth naming, because the
    contract's own columns ARE a list.
    """
    if not isinstance(document, dict):
        raise TypeError(f"a mapping is a mapping, got {type(document).__name__}")

    columns = document.get("columns")
    if isinstance(columns, list):
        raise ValueError(
            "mapping `columns` is keyed BY CONTRACT COLUMN NAME, not a list: "
            "`columns: {<contract column>: {from: <source column>}}`. The "
            "contract's columns are a list; a mapping's are not."
        )

    return Mapping.model_validate(document)

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
}


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
        description="`coalesce`: used when every source is null. Optional.",
    )

    description: Optional[str] = Field(
        default=None,
        description="Carried onto the emitted model's column documentation.",
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
        if wanted == "2+" and count < 2:
            raise ValueError(
                f"transform {self.transform!r} takes two or more `from`, got {count}"
            )

        for argument in spec["requires"]:
            if getattr(self, argument, None) is None:
                raise ValueError(
                    f"transform {self.transform!r} requires `{argument}`"
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

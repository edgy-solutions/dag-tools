"""The contract, read as data.

A contract states what an asset must carry for a governed read to accept it:
its grain, its scope, its period, and one row per column with that column's
type and the invariants that hold over it. ADR-0066's first one lives at
`contracts/ev_fixture.yaml` in invincible-agent; a deployment's own contract
is its own file.

**This module names no column, no table and no level.** It gives the *types*
a contract is written in, and every value arrives from the document. The
standing rule: dag-tools ships the types and the validators, never a
customer's shape.

**Reading is deliberately tolerant.** A contract carries sections this
library does not act on -- `rules` (the ids a reader refuses under), `limits`,
`tags` -- and new ones will arrive without a dag-tools release. Every model
here is `extra="ignore"`, so an unknown key is carried past rather than
rejected. The parts this library *does* act on are strict about their own
shape.

**Three column states, not two.** `required: true` is the reader's fixed
set. `produced_since: null` means declared but not yet produced -- the column
is in the contract so that readers can be written against it, and a mapping
is not wrong for leaving it unmapped. Collapsing those two into "optional"
would let a mapping skip a column the reader selects on every read, and the
operator would find out from a 502.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

# The type vocabulary a contract column is written in. Not a customer's
# shape -- these are the kinds a governed read distinguishes, and they are
# exactly the ones the `wrong_type` rule is stated over ("a string column is
# not a string, or a numeric column is not numeric").
#
# `numeric` is deliberately wider than `integer`: it accepts an integer or a
# float and says which one it is NOT. A mapping onto a `numeric` column
# therefore preserves the source's own int-ness instead of forcing a float --
# see `plan.py`, where coercing one amount column to float was enough to stop
# a mapping reproducing its fixture byte for byte.
CONTRACT_TYPES = ("string", "numeric", "integer", "float", "boolean")


class ContractColumn(BaseModel):
    """One column row of a contract."""

    model_config = ConfigDict(extra="ignore")

    name: str
    type: str = Field(description="One of CONTRACT_TYPES, as the document spells it.")
    unit: Optional[str] = None

    required: bool = Field(
        default=False,
        description=(
            "Every governed read selects this column. A mapping that does "
            "not produce it is refused."
        ),
    )
    produced_since: Optional[str] = Field(
        default=None,
        description=(
            "The release whose asset carries it. Null means declared but not "
            "yet produced -- a mapping may leave it unmapped."
        ),
    )

    constant_within: List[str] = Field(
        default_factory=list,
        description=(
            "The columns inside which every row carries one value. Each entry "
            "becomes a uniqueness test over this column."
        ),
    )
    additive_over: Optional[List[str]] = Field(
        default=None,
        description=(
            "The axes along which summing is meaningful. Read-side "
            "information: this library carries it into the catalog and tests "
            "nothing with it, because whether a sum is meaningful is not a "
            "property of the rows."
        ),
    )

    @property
    def declared_not_produced(self) -> bool:
        """True when the contract declares the column but nothing makes it yet."""
        return self.produced_since is None


class ContractPeriod(BaseModel):
    """Which column holds the period, and which calendar it is drawn from."""

    model_config = ConfigDict(extra="ignore")

    column: Optional[str] = None
    vocabulary: Optional[str] = Field(
        default=None,
        description=(
            "The NAME of the calendar, in the reading engine's vocabulary -- "
            "not its values. A name cannot be tested against; see `values`."
        ),
    )
    values: List[str] = Field(
        default_factory=list,
        description=(
            "The calendar itself, when the document carries it. Only then can "
            "a calendar test be emitted; a vocabulary name alone yields a note "
            "saying the test was not written, never a test that passes "
            "vacuously."
        ),
    )


class ContractDataset(BaseModel):
    """What the contract says about the dataset that satisfies it."""

    model_config = ConfigDict(extra="ignore")

    urn: Optional[str] = Field(
        default=None,
        description=(
            "The asset of record, as the PHYSICAL platform's dataset URN -- "
            "never a `dagster` platform URN (ruled 2026-11-05). The catalog "
            "registers under the physical platform and carries the Dagster "
            "asset as lineage."
        ),
    )
    producer: Dict[str, Any] = Field(default_factory=dict)
    storage: Optional[str] = Field(
        default=None,
        description="`file` or `table`. A table-backed read runs SELECT; a file-backed one does not.",
    )
    banner: Optional[str] = None


class ContractCadence(BaseModel):
    """How the rows are refreshed."""

    model_config = ConfigDict(extra="ignore")

    set_by: Optional[str] = None
    write: Optional[str] = Field(
        default=None,
        description=(
            "`whole_table` or an incremental form. Carried so a mapping that "
            "declares a different write mode is refused rather than silently "
            "producing rows the reader will see as duplicates."
        ),
    )


class Contract(BaseModel):
    """A whole contract document.

    Only the sections this library acts on are modelled. `rules`, `limits`
    and `tags` are read-side; they are kept as plain dicts so they survive a
    round trip and can be surfaced on the catalog without this module
    growing an opinion about them.
    """

    model_config = ConfigDict(extra="ignore")

    contract_id: str
    version: Optional[str] = None
    status: Optional[str] = None
    ratified_by: Optional[str] = None

    dataset: ContractDataset = Field(default_factory=ContractDataset)
    grain: List[str] = Field(
        default_factory=list,
        description="One row per tuple of these columns. The key-uniqueness test is over exactly this list.",
    )
    scope: Optional[str] = Field(
        default=None,
        description=(
            "The column a read filters to one value of. A producer-side "
            "mapping must carry it so the reader can filter; whether a read "
            "kept another scope's rows is the reader's rule, not ours."
        ),
    )
    period: ContractPeriod = Field(default_factory=ContractPeriod)
    status_period: Optional[str] = None

    columns: List[ContractColumn] = Field(default_factory=list)

    rules: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Rule id -> what it refuses on. Tests this library emits reuse "
            "these ids verbatim so a failing test and a reader's 502 name the "
            "same rule."
        ),
    )
    limits: Dict[str, Any] = Field(default_factory=dict)
    cadence: ContractCadence = Field(default_factory=ContractCadence)
    tags: Dict[str, Any] = Field(default_factory=dict)

    # -- derived views, so no caller re-derives them slightly differently --

    @property
    def column_names(self) -> List[str]:
        return [column.name for column in self.columns]

    def column(self, name: str) -> Optional[ContractColumn]:
        for candidate in self.columns:
            if candidate.name == name:
                return candidate
        return None

    @property
    def must_produce(self) -> List[ContractColumn]:
        """Columns a mapping is refused for not producing.

        `required: true` and actually produced. A required column whose
        `produced_since` is null is a contract looking forward -- it is
        reported as a note, not as a failure, because no release makes it yet.
        """
        return [
            column
            for column in self.columns
            if column.required and not column.declared_not_produced
        ]

    @property
    def unknown_types(self) -> List[str]:
        """Column names whose declared type this library does not know.

        Returned rather than raised: a contract may legitimately be ahead of
        this library, and the mapping for every other column is still worth
        validating. The plan turns this into an error only for a column the
        mapping actually produces.
        """
        return [c.name for c in self.columns if c.type not in CONTRACT_TYPES]


def load_contract(document: Dict[str, Any]) -> Contract:
    """Validate a parsed contract document.

    Takes a dict, never a path: this module does no IO, so the component can
    read the file and a test can hand over a literal. A `columns` entry that
    is not a mapping is the one shape worth a pointed error, because a YAML
    list of bare names is the obvious thing to write by hand.
    """
    if not isinstance(document, dict):
        raise TypeError(
            f"a contract is a mapping, got {type(document).__name__}"
        )

    columns = document.get("columns")
    if isinstance(columns, list):
        for index, entry in enumerate(columns):
            if not isinstance(entry, dict):
                raise ValueError(
                    f"contract column {index} is {type(entry).__name__}, not a "
                    "mapping; each column is a row with at least `name` and `type`"
                )

    return Contract.model_validate(document)

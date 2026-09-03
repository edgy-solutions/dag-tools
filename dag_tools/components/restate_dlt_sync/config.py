"""Validated config shapes for :class:`RestateDltSyncComponent`.

These live apart from the component so the YAML contract is readable in
one place, and so a typo in a twelve-table pipeline fails at definitions
load with a field name rather than at 3am with a KeyError.

Everything a site might name differently is a field. That is deliberate
and not over-engineering: the PDM control table, its status column, the
strings that column holds, the load-type column and ITS strings, the
timestamp column, the MEI table and the column MEIs are itemized in --
all of it varies per deployment, and none of it can be guessed.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TableSpec(BaseModel):
    """Per-table extraction settings.

    PDM does one full load and then only deltas, so every table needs
    both halves: an ``index`` to merge on and a ``cursor`` to advance.
    Declaring them per table (rather than one primary key for the whole
    pipeline, which is all the component used to accept) is what makes a
    dozen heterogeneous tables expressible.
    """
    model_config = ConfigDict(extra="forbid")

    primary_key: Union[str, List[str]] = Field(
        description="Merge/index key. A list for a composite key.",
    )
    cursor: Optional[str] = Field(
        default=None,
        description=(
            "Incremental cursor column. Omit ONLY for a table that is "
            "always fully reloaded -- without it every run re-extracts "
            "the whole table, which is correct but not incremental."
        ),
    )
    initial_value: Optional[Any] = Field(
        default=None,
        description=(
            "Starting cursor value. Left unset the first run is unbounded, "
            "which is exactly the full load PDM does first."
        ),
    )


class ControlTableSpec(BaseModel):
    """The control table both sides write to.

    PDM appends rows describing its own progress (started, then
    completed, tagged FULL or DELTA). We append one row of our own once
    the data is on our side. Reading it top to bottom gives the whole
    history of a cycle from both directions.

    This table is also the completion handshake. Polling the staging
    tables for row count cannot distinguish "PDM is done" from "PDM is
    a third of the way through committing", so a cycle driven off row
    counts will extract a partial load and acknowledge it. The COMPLETED
    row is the only trustworthy signal that a load is whole.
    """
    model_config = ConfigDict(extra="forbid")

    name: str = Field(description="Control table name.")
    status_column: str = Field(description="Column holding the status string.")
    completed_value: str = Field(
        description="Status PDM writes when its load is finished and whole.",
    )
    consumer_done_value: str = Field(
        description=(
            "Status WE write once the data is loaded on our side. Must "
            "differ from completed_value, or our own row would look like "
            "a fresh load from PDM and the cycle would never settle."
        ),
    )
    timestamp_column: str = Field(
        description=(
            "Load timestamp column. Required, because it is how a cycle "
            "decides whether a COMPLETED load has already been consumed: "
            "the latest COMPLETED is compared against the latest "
            "consumer-done row. Without an ordering there is no way to "
            "tell a new load from the one just finished."
        ),
    )

    started_value: Optional[str] = Field(
        default=None,
        description="Status the source writes when it begins. Recorded for operators.",
    )
    consumer_started_value: Optional[str] = Field(
        default=None,
        description=(
            "Status WE write before reading anything. Setting it turns the "
            "control table into a GATE: the source polls for this and holds "
            "off updating the data until we close the cycle. Leave it unset "
            "and no start marker is written at all."
        ),
    )
    consumer_aborted_value: Optional[str] = Field(
        default=None,
        description=(
            "Status WE write when a cycle fails. Required whenever "
            "consumer_started_value is set, because the start marker is a "
            "lock and only a terminal row releases it. Without this a failed "
            "run leaves the source blocked indefinitely."
        ),
    )
    load_type_column: Optional[str] = Field(
        default=None,
        description="Column holding FULL / DELTA for the load.",
    )
    full_value: str = Field(default="FULL")
    delta_value: str = Field(default="DELTA")
    extra_columns: Dict[str, Any] = Field(
        default_factory=dict,
        description="Constant columns added to the row we write.",
    )

    @model_validator(mode="after")
    def _distinct_statuses(self) -> "ControlTableSpec":
        if self.consumer_done_value == self.completed_value:
            raise ValueError(
                "control_table.consumer_done_value must differ from "
                f"completed_value (both are {self.completed_value!r}); "
                "otherwise the row we write to close a cycle is "
                "indistinguishable from the source announcing a new one, and "
                "the sensor re-fires forever."
            )

        if self.consumer_started_value and not self.consumer_aborted_value:
            raise ValueError(
                "control_table.consumer_started_value is set without "
                "consumer_aborted_value. The start marker is a LOCK -- the "
                "source holds off updating the data until a terminal row from "
                "us releases it -- and only the success path writes "
                f"{self.consumer_done_value!r}. Without an aborted value a "
                "failed run leaves the source blocked with nothing to clear it."
            )

        # Every status we write must be distinguishable from every other and
        # from theirs, or one side reads the other's markers as its own.
        seen: Dict[str, str] = {}
        for field in (
            "consumer_started_value",
            "consumer_done_value",
            "consumer_aborted_value",
        ):
            value = getattr(self, field)
            if value is None:
                continue
            if value in seen:
                raise ValueError(
                    f"control_table.{field} and control_table.{seen[value]} "
                    f"are both {value!r}. Each status we write must be "
                    f"distinct, or a cycle cannot tell its own start from its "
                    f"own end."
                )
            seen[value] = field
            if value in (self.completed_value, self.started_value):
                raise ValueError(
                    f"control_table.{field} is {value!r}, which is also a "
                    f"status the SOURCE writes. Ours must be distinguishable "
                    f"from theirs, or the two sides read each other's markers."
                )
        return self


class MeiTableSpec(BaseModel):
    """The table we write top-level Major End Items into.

    Writing this is what starts the transaction: PDM reads the MEIs,
    explodes them, and fills the staging tables.

    Only some tables are MEI-scoped, but nothing here distinguishes
    them. A table that is not MEI-driven simply populates regardless, and
    one that is stays empty until the MEI list is written -- so treating
    every table identically costs nothing and keeps the config honest
    about what it does not know.
    """
    model_config = ConfigDict(extra="forbid")

    name: str = Field(description="MEI table name.")
    mei_column: Optional[str] = Field(
        default=None,
        description=(
            "Column the identifiers are itemized in. Required when the "
            "request list is a list of bare values; unnecessary when each "
            "entry is a mapping that names its own columns."
        ),
    )
    constants: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Columns identical on every row, and NOT overridable by an "
            "entry -- so a value that must be uniform cannot be varied by "
            "editing the request list."
        ),
    )
    defaults: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Columns applied per row and overridable by an entry. For a "
            "value that is usually one thing but occasionally stated "
            "explicitly."
        ),
    )
    source_file: Optional[str] = Field(
        default=None,
        description=(
            "Path to the MEI list, supplied as a git-repo overlay mounted "
            "into the pod. Read at materialization time, not at "
            "definitions load, so re-pointing the overlay takes effect on "
            "the next run instead of the next redeploy. Accepts a YAML "
            "list, a JSON list, or one MEI per line with # comments."
        ),
    )
    meis: List[Any] = Field(
        default_factory=list,
        description=(
            "Inline request list. Bare values, or mappings naming their own "
            "columns. Useful for tests and small fixed sets; source_file "
            "wins when both are set."
        ),
    )
    replace: bool = Field(
        default=True,
        description=(
            "Clear the table before inserting, so it states the CURRENT "
            "request rather than the union of every request ever made."
        ),
    )
    extra_columns: Dict[str, Any] = Field(
        default_factory=dict,
        description="Deprecated alias for `constants`, kept working.",
    )

    @model_validator(mode="after")
    def _addressable_columns(self) -> "MeiTableSpec":
        """Every row has to know which column its identifier goes in.

        A mapping entry names its own columns, so this only binds when the
        list can contain bare values -- which is the common case and the
        one where a missing column name is silently unrecoverable.
        """
        if self.mei_column:
            return self
        entries = self.meis or []
        if entries and all(isinstance(e, dict) for e in entries):
            return self
        if self.source_file:
            # The file is read at materialization time, so its shape is not
            # knowable here; the loader raises if it turns out to be bare
            # values with no column to put them in.
            return self
        raise ValueError(
            "mei_table needs `mei_column` unless every entry is a mapping "
            "that names its own columns."
        )

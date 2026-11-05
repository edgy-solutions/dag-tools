"""Dagster-facing configuration schema for the read audit.

Per `AGENTS.md` §5 the component exposes a Dagster-centric schema and
translates it into what the engine wants; consumers never hand raw kwargs
through. The component's own fields are `Dict[str, Any]` (the house shape --
nested `Resolvable` models behave differently across the versions this library
supports, down to the 1.10.19 compat floor), and these models are what those
dicts are validated against at component load. A malformed block therefore
fails the code location load with a pointed error rather than at 3am inside a
materialization.

Everything an operator could want to name lives here as a required field with
no default: the history relation, and the five columns it is read through.
A default would put an object name in this repo, and a wrong default would
audit the wrong column in silence. See `dag_tools.audit.history`.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from dagster import Config
from pydantic import BaseModel, ConfigDict, Field, model_validator


class HistoryBlock(BaseModel):
    """Where the query-history rows come from.

    Exactly one of two topologies:

      * `rows_file` -- the operator exported history themselves and this reads
        the file. The first pass is usually this: the warehouse account that
        can read history is often not the one Dagster holds, and nothing has
        to be granted to start measuring.
      * `resource_key` -- a resource in the Definitions fetches the rows. The
        component builds the statement (`audit.history.build_history_statement`)
        and hands it over; the resource executes it. That is the seam step 2's
        tagged warehouse resource plugs into.
    """

    model_config = ConfigDict(extra="forbid")

    rows_file: Optional[str] = Field(
        default=None,
        description=(
            "Path to exported history rows, as JSON. Relative paths resolve "
            "against the component directory."
        ),
    )

    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Resource that executes the history read. It must expose "
            "`fetch_rows(statement, parameters)` returning a list of mappings."
        ),
    )
    relation: Optional[str] = Field(
        default=None,
        description="The history relation, as this account spells it.",
    )
    columns: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Which column holds each of query_id, started_at, principal, "
            "query_tag, relations. All five required when reading through a "
            "resource."
        ),
    )
    principal_filter: Optional[str] = Field(
        default=None,
        description=(
            "Restrict to one identity -- the service identity being audited. "
            "Unset reads every identity, and the report separates pipeline "
            "reads from the rest by principal."
        ),
    )
    lookback_hours: int = Field(
        default=168,
        gt=0,
        description="How far back the read reaches, from the run's start.",
    )

    @model_validator(mode="after")
    def _exactly_one_topology(self) -> "HistoryBlock":
        from_file = self.rows_file is not None
        from_resource = self.resource_key is not None
        if from_file == from_resource:
            raise ValueError(
                "history must declare exactly one of 'rows_file' (read an "
                "export) or 'resource_key' (have a resource run the read)"
            )
        if from_resource:
            if not self.relation:
                raise ValueError(
                    "history.relation is required when reading through a "
                    "resource -- this library does not know how your account "
                    "spells its query history, and will not guess"
                )
            missing = [
                name
                for name in ("query_id", "started_at", "principal", "query_tag", "relations")
                if not (self.columns or {}).get(name)
            ]
            if missing:
                raise ValueError(
                    "history.columns must name every column the audit reads; "
                    f"missing: {', '.join(missing)}"
                )
        return self


class RunsBlock(BaseModel):
    """Where the run windows come from, for reads no tag explains.

    Both sources may be used together; windows are concatenated. With neither,
    nothing correlates by time and every untagged read stays in the gap --
    which is the honest answer, not a degraded one.
    """

    model_config = ConfigDict(extra="forbid")

    runs_file: Optional[str] = Field(
        default=None,
        description="Path to run windows as JSON (run_id, asset_keys, started_at, ended_at).",
    )
    from_instance: bool = Field(
        default=False,
        description=(
            "Read run windows from the Dagster instance this asset runs in. "
            "Only runs carrying an asset selection can attribute anything; a "
            "run that selected nothing in particular stays ambiguous."
        ),
    )
    limit: int = Field(
        default=2000,
        gt=0,
        description="Most recent runs to consider when reading the instance.",
    )


class ReadAuditRunConfig(Config):
    """Per-run knobs, settable from the launchpad.

    Deploy-time shape (where history lives, how it is spelled) is YAML;
    everything here is a decision an operator makes for one run.
    """

    lookback_hours: int = Field(
        default=0,
        description="Override the configured lookback for this run (0 = use YAML).",
    )
    max_listed: int = Field(
        default=200,
        description=(
            "Cap how many object identifiers each list in the materialization "
            "metadata shows. The report itself is never truncated; this only "
            "keeps the UI readable. 0 lists none."
        ),
    )


class ReadAuditSpec(BaseModel):
    """The validated whole, as `build_defs` resolves it."""

    model_config = ConfigDict(extra="forbid")

    asset_name: str = "read_audit"
    group_name: Optional[str] = None
    io_manager_key: Optional[str] = None
    owners: List[str] = Field(default_factory=list)
    tags: Dict[str, str] = Field(default_factory=dict)
    history: HistoryBlock
    runs: RunsBlock = Field(default_factory=RunsBlock)

    @classmethod
    def from_attributes(
        cls,
        *,
        asset_name: str,
        group_name: Optional[str],
        io_manager_key: Optional[str],
        owners: Optional[List[str]],
        tags: Optional[Dict[str, str]],
        history: Dict[str, Any],
        runs: Optional[Dict[str, Any]],
    ) -> "ReadAuditSpec":
        return cls(
            asset_name=asset_name,
            group_name=group_name,
            io_manager_key=io_manager_key,
            owners=list(owners or []),
            tags=dict(tags or {}),
            history=HistoryBlock.model_validate(history or {}),
            runs=RunsBlock.model_validate(runs or {}),
        )

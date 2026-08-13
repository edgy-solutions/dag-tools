"""Dagster-facing configuration schema for the OTel -> API component.

Per `AGENTS.md` §5 the component exposes a Dagster-centric schema and
translates it into what dlt, ClickHouse and Restate actually want —
consumers never hand raw dlt kwargs through.
"""
from typing import Any, Dict, List, Optional

from dagster import Config
from pydantic import BaseModel, Field


class OtelApiSyncRunConfig(Config):
    """Run-time knobs, settable from the Dagster launchpad.

    Deploy-time shape (queries, mapping, endpoints) lives in the
    component YAML; everything here is a per-run decision an operator
    makes when investigating or backfilling.
    """

    dry_run: bool = Field(
        default=False,
        description=(
            "Render and log the exact plans without sending them. The full "
            "payloads land in asset metadata, which is the intended way to "
            "review a mapping change before it reaches the API."
        ),
    )
    limit: int = Field(default=0, description="Cap rows read from the source (0 = no cap).")
    max_groups: int = Field(default=0, description="Cap execution groups dispatched (0 = no cap).")
    only_group: Optional[str] = Field(
        default=None, description="Dispatch just this execution group key."
    )
    ignore_readiness: bool = Field(
        default=False,
        description=(
            "Bypass the readiness gate. Dispatches groups that may still be "
            "filling — for recovering stranded groups, not routine runs."
        ),
    )
    ignore_ledger: bool = Field(
        default=False,
        description=(
            "Re-send groups already recorded as dispatched. The Restate object "
            "still refuses plan hashes it has completed, so this replays only "
            "genuinely changed plans."
        ),
    )


class LedgerSchema(BaseModel):
    enabled: bool = Field(default=True)
    backend: Optional[str] = Field(
        default=None,
        description="'sql' (staging destination table) or 'dagster' (asset metadata). "
        "Defaults to 'sql' when staged, 'dagster' otherwise.",
    )
    table: str = Field(default="otel_api_dispatch_ledger")
    schema_name: Optional[str] = Field(default=None, alias="schema")

    class Config:
        populate_by_name = True


class ClickHouseResourceSchema(BaseModel):
    """One extraction query against ClickHouse."""

    name: str = Field(description="Resource name; also the staged table name.")
    query: Optional[str] = Field(default=None, description="Full SELECT to extract.")
    table: Optional[str] = Field(default=None, description="Table name, when not using 'query'.")
    where: Optional[str] = Field(default=None)
    columns: Optional[List[str]] = Field(default=None)
    cursor_column: Optional[str] = Field(
        default="Timestamp", description="Incremental cursor column."
    )
    lookback_seconds: int = Field(
        default=0, description="Re-read window for late-arriving telemetry."
    )
    primary_key: Optional[List[str]] = Field(default=None)
    write_disposition: Optional[str] = Field(default=None)
    map_columns: Optional[List[str]] = Field(
        default=None, description="Columns to force to JSON type (OTel attribute maps)."
    )
    limit: int = Field(default=0)


class OtelApiSyncPipelineSchema(BaseModel):
    """One ClickHouse-to-API pipeline."""

    name: Optional[str] = Field(default=None)
    staged: bool = Field(
        default=True,
        description=(
            "Stage through the destination with dlt before dispatching. "
            "Gives replay, an incremental cursor and a durable ledger. Set "
            "false to read ClickHouse directly in the dispatch asset."
        ),
    )
    sources: List[ClickHouseResourceSchema] = Field(default_factory=list)
    mapping_file: Optional[str] = Field(
        default=None, description="Path to the mapping YAML, relative to the component directory."
    )
    mapping: Optional[Dict[str, Any]] = Field(
        default=None, description="Inline mapping document, as an alternative to mapping_file."
    )
    dest_schema: Optional[str] = Field(default=None)
    dispatch_from: Optional[str] = Field(
        default=None,
        description="Which source resource the dispatch asset reads. Defaults to the first.",
    )
    max_plan_bytes: int = Field(
        default=4 * 1024 * 1024,
        description=(
            "Reject a rendered plan larger than this instead of letting the "
            "Restate ingress refuse it mid-dispatch. Plans run about 390 bytes "
            "per call, so ~10k calls per MiB. `max_groups` does not help here: "
            "the problem case is one *large* execution group, which is a single "
            "plan to a single object key. Narrow the source query, split the "
            "group, or raise both this and the ingress body limit. 0 disables."
        ),
    )
    ledger: LedgerSchema = Field(default_factory=LedgerSchema)
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    io_manager_key: str = Field(default="io_manager")
    pipeline_kwargs: Dict[str, Any] = Field(default_factory=dict)


class OtelApiSyncSchema(BaseModel):
    """Root schema for `OtelApiSyncComponent`."""

    source_config: Dict[str, Any] = Field(description="ClickHouse connection configuration.")
    restate_endpoint: str = Field(
        description=(
            "Restate ingress base URL. The component appends "
            "/ApiCallPlanService/<group>/execute_plan/send."
        )
    )
    dest_config: Optional[Dict[str, Any]] = Field(
        default=None, description="Staging destination; required when any pipeline is staged."
    )
    staging_config: Optional[Dict[str, Any]] = Field(default=None)
    pipelines: Dict[str, OtelApiSyncPipelineSchema] = Field(default_factory=dict)

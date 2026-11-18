"""Dagster-facing configuration schema for the documents-to-tables component.

The component's own fields are plain dicts and scalars (the house shape --
nested `Resolvable` models behave differently across the versions this
library supports, down to the 1.10.19 compat floor), and these models are
what those dicts are validated against at component load, so a malformed
block fails the code location load with a pointed error rather than inside
a materialization.

**Nothing here names a document format, a table or a column.** The mapping
is the deployment's file; this schema says where bundles come from, where
tables go, and how strict the checks are.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SourceBlock(BaseModel):
    """Where bundles come from: a local directory or ``s3://bucket/prefix``."""

    model_config = ConfigDict(extra="forbid")

    uri: str = Field(description="A local directory or s3://bucket/prefix holding the bundles.")
    endpoint_url: Optional[str] = Field(
        default=None, description="S3-compatible endpoint (MinIO, etc.). Credentials come from the environment."
    )
    bundle_depth: int = Field(
        default=0,
        ge=0,
        description=(
            "How many directory levels under the root name a tree bundle. 0: the whole "
            "root is one bundle (lists the entire source every time; unsuitable for large "
            "trees); D >= 1: each directory at exactly depth D is a tree bundle and a .zip at "
            "depth <= D is its own bundle. A non-zip file at depth <= D is not a bundle and is "
            "only counted. A .zip inside a tree bundle is a member, expanded one level. "
            "Discovery lists only the first D levels."
        ),
    )

    @field_validator("uri")
    @classmethod
    def _scheme(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("source.uri is required")
        if "://" in v and not v.startswith(("s3://", "file://")):
            raise ValueError(f"source.uri must be s3:// or a local path, not {v.split('://')[0]}://")
        return v


class DestinationBlock(BaseModel):
    """Where tables go: one Lance dataset per table at ``<uri>/<table>.lance``."""

    model_config = ConfigDict(extra="forbid")

    uri: str = Field(description="A local directory or an object-store URI Lance accepts.")
    storage_options: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Passed to Lance as-is. Use `{{ env('X') }}` for anything secret; nothing "
            "here is logged or put on a materialization."
        ),
    )
    flush_rows: int = Field(default=50_000, ge=1, description="Rows buffered per table before a write.")
    flush_attachment_bytes: int = Field(
        default=256 * 1024 * 1024, ge=1, description="Attachment bytes buffered per table before a write."
    )

    @field_validator("uri")
    @classmethod
    def _present(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("destination.uri is required")
        return v


class ChecksBlock(BaseModel):
    """Thresholds for the asset checks. Strict by default; every check WARNs."""

    model_config = ConfigDict(extra="forbid")

    max_reject_rate: float = Field(
        default=0.0, ge=0.0, le=1.0, description="Largest share of a bundle's documents that may be rejected."
    )
    min_file_resolution_rate: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Smallest share of a table's file references that must resolve to exactly one member.",
    )
    unknown_paths: int = Field(
        default=0, ge=0, description="Most structural paths outside expected_paths a bundle may carry."
    )
    allow_empty: List[str] = Field(
        default_factory=list,
        description="Tables a bundle may legitimately produce no row for; their non-empty check is skipped.",
    )


class DiscoveryBlock(BaseModel):
    """The sensor that adds a partition per new bundle and requests its run."""

    model_config = ConfigDict(extra="forbid")

    interval_seconds: int = Field(default=300, ge=1)
    max_new_partitions_per_tick: int = Field(default=25, ge=1)
    default_status: str = Field(default="STOPPED", description="RUNNING or STOPPED. Stopped unless asked.")
    start_after: Optional[str] = Field(
        default=None,
        description=(
            "Bundles whose key is <= this are not discovered, and the listing skips them where "
            "the source allows (StartAfter on S3). Keys compare as strings."
        ),
    )

    @field_validator("default_status")
    @classmethod
    def _status(cls, v: str) -> str:
        v = str(v).upper()
        if v not in ("RUNNING", "STOPPED"):
            raise ValueError("discovery.default_status must be RUNNING or STOPPED")
        return v


class MaintenanceBlock(BaseModel):
    """Compaction and old-version cleanup for every table."""

    model_config = ConfigDict(extra="forbid")

    older_than_days: float = Field(default=7.0, gt=0, description="Versions older than this are removed.")
    retain_versions: Optional[int] = Field(default=None, ge=1, description="Versions kept regardless of age.")
    cron: Optional[str] = Field(default=None, description="A schedule for the maintenance job; it starts STOPPED.")
    default_status: str = Field(default="STOPPED")

    @field_validator("default_status")
    @classmethod
    def _status(cls, v: str) -> str:
        v = str(v).upper()
        if v not in ("RUNNING", "STOPPED"):
            raise ValueError("maintenance.default_status must be RUNNING or STOPPED")
        return v


class DocumentTablesSpec(BaseModel):
    """Everything the component's attributes say, validated."""

    model_config = ConfigDict(extra="forbid")

    name: str
    source: SourceBlock
    mapping: Any
    destination: DestinationBlock
    asset_key_prefix: Optional[List[str]] = None
    checks: ChecksBlock = Field(default_factory=ChecksBlock)
    discovery: DiscoveryBlock = Field(default_factory=DiscoveryBlock)
    maintenance: MaintenanceBlock = Field(default_factory=MaintenanceBlock)
    write_concurrency: int = Field(default=1, ge=1)
    redact: bool = False
    group_name: Optional[str] = None
    owners: List[str] = Field(default_factory=list)
    tags: Dict[str, str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        if not _NAME_RE.match(v or ""):
            raise ValueError("name must be an identifier (letters, digits, underscores)")
        return v

    @field_validator("asset_key_prefix")
    @classmethod
    def _prefix(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is None:
            return v
        if not v:
            raise ValueError("asset_key_prefix, when given, must name at least one part")
        bad = [p for p in v if not _NAME_RE.match(str(p))]
        if bad:
            raise ValueError(f"asset_key_prefix parts must be identifiers: {bad}")
        return [str(p) for p in v]

    @model_validator(mode="after")
    def _mapping_shape(self) -> "DocumentTablesSpec":
        if not isinstance(self.mapping, (str, dict)) or not self.mapping:
            raise ValueError("mapping must be a path to a mapping file or the mapping itself, inline")
        return self

    @property
    def prefix(self) -> List[str]:
        return list(self.asset_key_prefix) if self.asset_key_prefix else [self.name]

    @classmethod
    def from_attributes(cls, **attrs: Any) -> "DocumentTablesSpec":
        """Validate the component's attributes, naming every problem at once."""
        return cls.model_validate({k: v for k, v in attrs.items() if v is not None})


__all__ = [
    "ChecksBlock",
    "DestinationBlock",
    "DiscoveryBlock",
    "DocumentTablesSpec",
    "MaintenanceBlock",
    "SourceBlock",
]

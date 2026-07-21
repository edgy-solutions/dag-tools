"""Pydantic schemas for the qualification manifest.

The manifest is the **single source of truth** for one qualification run.
It captures:

  * The version pair under test (baseline vs. candidate).
  * Frozen inventory pins — exactly which (repo, sha) pairs every later
    phase will operate on, so the qualification is immune to builds that
    land mid-qualification.
  * Co-upgrade risks — non-Dagster libraries whose pins changed between
    baseline and candidate (notably dbt-core, dbt adapters, warehouse
    clients). These must be separately validated or pinned back; otherwise
    a hidden dbt-core regression masquerades as a Dagster failure.
  * Test deployment GraphQL endpoint + staging resource override config.
  * Selection knobs for representative picking in Q1.

Evolution rules mirror the rest of the qualification system (recipe ADR-2):

  * Add only ``Optional[...]`` fields with defaults; never rename or remove.
  * Bump ``SCHEMA_VERSION`` in the same commit.
  * Readers use ``extra="ignore"`` so unknown fields are silently tolerated.

The manifest is persisted as YAML (human-readable; mirrors the recipe's
sample) but the on-disk shape is just ``QualificationManifest.model_dump()``.
"""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


SCHEMA_VERSION = 1
"""Bump on every additive change to the manifest schema."""


class VersionTarget(BaseModel):
    """One side of the qualification version pair.

    ``dagster`` is the Dagster version being tested (baseline or candidate);
    ``pins`` records every other library version that matters for parity
    — notably the dagster-* family (dagster-dbt, dagster-k8s, dagster-aws),
    dbt-core, dbt adapters, and warehouse clients. These get diffed
    across sides to surface ``co_upgrade_risks``.
    """
    model_config = ConfigDict(extra="ignore")

    dagster: str
    pins: Dict[str, str] = Field(default_factory=dict)


class InventoryPin(BaseModel):
    """One frozen (repo, sha) pair from the registry at qualification-init time.

    Captures the timestamp from ``latest.json`` for operator audit trail —
    "this qualification pinned repo R at the build that was current at
    14:32 UTC on 2026-06-15".
    """
    model_config = ConfigDict(extra="ignore")

    repo: str
    git_sha: str
    pinned_timestamp: Optional[datetime] = None


class CoUpgradeRisk(BaseModel):
    """A non-Dagster library whose version changes between baseline and candidate.

    The qualification verdict (Q6) refuses to GO unless every risk here is
    either separately validated or explicitly pinned back to the baseline
    version. Otherwise a hidden dbt-core/warehouse regression would
    propagate as a false-positive Dagster failure.
    """
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    lib: str
    from_version: str = Field(alias="from")
    to_version: str = Field(alias="to")
    severity: str = Field(default="warning")
    """One of ``"warning"`` (minor/patch) or ``"major"`` (major bump)."""


class Deployment(BaseModel):
    """k8s test deployment connection details for Q2/Q3/Q4 GraphQL launches."""
    model_config = ConfigDict(extra="ignore")

    graphql_url: Optional[str] = None
    auth: Optional[str] = None
    """Auth scheme. ``"env:VAR_NAME"`` reads a bearer token from env.
    Future: ``"oauth:..."``, ``"none"``. Phase 2 implementation will
    grow this; the manifest just records the operator intent."""

    location_name: Optional[str] = None
    """Code-location name to target when Q2/Q4 launch RUNNABLE
    representatives, exactly as the test deployment's workspace surfaces
    it (e.g. the user-deployment / gRPC server name, or ``demo_defs.py``
    for a local ``dagster dev -f demo_defs.py``). When unset the launcher
    falls back to ``"default"``, which only works if the deployment
    happens to name its location that — real deployments never do, so set
    this via ``dagtools qual init --location-name``."""

    job_name: Optional[str] = None
    """Job to launch representatives through. Defaults to Dagster's
    implicit ``"__ASSET_JOB"`` when unset, which is correct for
    asset-only locations."""


class Selection(BaseModel):
    """Representative-picking knobs consumed by Q1."""
    model_config = ConfigDict(extra="ignore")

    prefer_tag: str = Field(default="regression")
    """When tied for "best" within a class, prefer assets tagged with this."""

    reps_per_class: int = Field(default=2)
    """Number of representatives to pick per equivalence class. The recipe
    says ideally spanning >=2 repos per class — Q1 will best-effort that."""


class QualificationManifest(BaseModel):
    """The full qualification manifest.

    Written once at ``dagtools qual init`` time; read by every subsequent
    phase. Treated as **immutable** by the registry (Q1..Q6 add their own
    artifacts under ``qualifications/<qual_id>/`` but never rewrite the
    manifest — a re-run with the same qual_id is a refused operation
    unless ``--allow-overwrite`` is passed).
    """
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION)
    qual_id: str
    created_at: datetime

    baseline: VersionTarget
    candidate: VersionTarget

    co_upgrade_risks: List[CoUpgradeRisk] = Field(default_factory=list)
    inventory_pins: List[InventoryPin] = Field(default_factory=list)

    deployment: Deployment = Field(default_factory=Deployment)
    staging_overrides: Optional[str] = None
    """Pointer (typically ``s3://...``) to the staging resource override
    config. Phase 2 GraphQL launches pull this as run_config."""

    selection: Selection = Field(default_factory=Selection)

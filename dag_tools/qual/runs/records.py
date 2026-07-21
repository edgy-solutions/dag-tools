"""Per-run record persisted to ``qualifications/<qual_id>/<side>/runs/
<class_hash>/<run_id>.json``.

Recipe rule (Phase Q2 item 3):

  > Persist per run:
  >   {class_hash, asset_key, run_id, success, duration,
  >    materialization_events[], asset_check_results[], metadata_keys[],
  >    error (if any)}

The shape here is that record plus a ``schema_version`` and a few audit
fields. Same additive-only evolution rules as the rest of the system.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


SCHEMA_VERSION = 1


class MaterializationEventSummary(BaseModel):
    """Compact summary of one MaterializationEvent — enough for Q6 to diff
    "did baseline and candidate produce the same materializations" without
    persisting Dagster-internal blobs verbatim."""
    model_config = ConfigDict(extra="ignore")

    asset_key: List[str]
    step_key: Optional[str] = None
    timestamp: Optional[float] = None
    metadata_keys: List[str] = Field(default_factory=list)


class AssetCheckResultSummary(BaseModel):
    """Compact summary of one asset-check result. Q6 diffs pass/fail parity."""
    model_config = ConfigDict(extra="ignore")

    asset_key: List[str]
    check_name: Optional[str] = None
    passed: Optional[bool] = None
    severity: Optional[str] = None


class RunRecord(BaseModel):
    """The persisted record of one representative's run on one side."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION)

    # Identity / lineage
    qual_id: str
    side: str
    class_hash: str
    asset_key: List[str]
    repo: str
    git_sha: str

    # The Dagster run
    run_id: str
    success: bool
    status: str
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    # What we saw
    materialization_events: List[MaterializationEventSummary] = Field(default_factory=list)
    asset_check_results: List[AssetCheckResultSummary] = Field(default_factory=list)
    metadata_keys: List[str] = Field(default_factory=list)
    """Union of all metadata keys observed across materialization events.
    Q6 diffs the *key set*; values may legitimately differ between
    baseline and candidate."""

    # Failure detail
    error: Optional[str] = None
    failure_step_keys: List[str] = Field(default_factory=list)

    # Raw count for sanity in summary reports
    event_count: int = 0

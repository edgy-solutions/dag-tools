"""Resumable per-representative run state for Q2/Q4.

Recipe rule:

  > `qual run` maintains ``~/.dagtools/quals/<qual_id>/state.json`` mirroring
  > to the registry: per-representative status
  > ``pending | launched(run_id) | passed | failed | skipped``. Re-invocation
  > processes only non-passed entries. If the desktop dies mid-run,
  > re-invoking reconciles ``launched`` entries by querying run status via
  > GraphQL rather than relaunching.

So this module:

  * Defines :class:`RepStatus` and :class:`RepState` (one per representative).
  * :class:`QualRunState` is the whole per-(qual_id, side) state file.
  * The file is BOTH local (operator scratch) and registry-mirrored. The
    operator's local copy is authoritative during a run; we sync to the
    registry on every state transition so a different desktop can recover
    if the original one dies.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


SCHEMA_VERSION = 1


class RepStatus(str, Enum):
    """Per-representative status during a Q2/Q4 side run."""

    PENDING = "pending"
    LAUNCHED = "launched"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


class RepState(BaseModel):
    """One representative's state at a moment in time.

    ``rep_id`` is a stable identifier we compute as
    ``"<class_hash>:<asset_key.with(slash)>"`` so retries against the same
    rep update the same entry deterministically.
    """
    model_config = ConfigDict(extra="ignore")

    rep_id: str
    class_hash: str
    asset_key: List[str]
    repo: str
    git_sha: str
    runnability: str

    status: RepStatus = Field(default=RepStatus.PENDING)
    run_id: Optional[str] = None
    last_updated: Optional[datetime] = None
    error: Optional[str] = None
    attempts: int = 0


class QualRunState(BaseModel):
    """The full per-(qual_id, side) state file.

    Stored locally at ``~/.dagtools/quals/<qual_id>/<side>-state.json`` and
    mirrored to ``s3://.../qualifications/<qual_id>/<side>/state.json``.
    """
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION)
    qual_id: str
    side: str
    started_at: datetime
    updated_at: datetime
    reps: Dict[str, RepState] = Field(default_factory=dict)


def rep_id_for(class_hash: str, asset_key: List[str]) -> str:
    """Stable identifier joining a class_hash and asset_key into one key."""
    return f"{class_hash}:{'/'.join(asset_key)}"


def default_local_state_path(qual_id: str, side: str) -> Path:
    """``~/.dagtools/quals/<qual_id>/<side>-state.json`` — convention used
    everywhere we need operator-scratch state."""
    home = Path(os.environ.get("DAGTOOLS_HOME") or (Path.home() / ".dagtools"))
    return home / "quals" / qual_id / f"{side}-state.json"


def utcnow() -> datetime:
    """Single UTC clock so tests can monkeypatch this module only."""
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# Selection helpers — drive which reps a re-invocation should process.
# ---------------------------------------------------------------------------


def pending_or_resumable(state: QualRunState) -> List[RepState]:
    """Reps that a fresh invocation should act on:
      * PENDING — never launched
      * LAUNCHED — desktop died after launch; reconcile via GraphQL
      * FAILED — retry policy lives in the caller; this just exposes them

    PASSED and SKIPPED are stable terminal states — left alone."""
    out: List[RepState] = []
    for rep in state.reps.values():
        if rep.status in (RepStatus.PENDING, RepStatus.LAUNCHED, RepStatus.FAILED):
            out.append(rep)
    return out


def passed(state: QualRunState) -> List[RepState]:
    return [r for r in state.reps.values() if r.status == RepStatus.PASSED]


def transition(
    rep: RepState,
    *,
    status: RepStatus,
    run_id: Optional[str] = None,
    error: Optional[str] = None,
    bump_attempts: bool = False,
) -> RepState:
    """Return a NEW RepState with the transition applied. Pydantic is fine
    with mutation but immutability-by-convention here makes diffs in
    logs trivially obvious."""
    fresh = rep.model_copy(update={
        "status": status,
        "run_id": run_id if run_id is not None else rep.run_id,
        "error": error,
        "last_updated": utcnow(),
        "attempts": rep.attempts + (1 if bump_attempts else 0),
    })
    return fresh

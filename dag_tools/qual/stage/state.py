"""Per-group state and per-run records for ``dagtools qual stage``.

Its own slot (``qualifications/<qual_id>/<side>/stage/``), with types
distinct from :class:`~dag_tools.qual.runs.state.RepState` and
:class:`~dag_tools.qual.probes.state.ProbeRepState`, so crossing a stage
path with a representative path fails at type-check time.

:class:`StageRecord` is deliberately NOT a ``RunRecord``: it describes no
equivalence class, so Q6's diff has nothing to key it on and cannot consume
one by accident.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from .plan import StagePlan


SCHEMA_VERSION = 1


class StageStatus(str, Enum):
    PENDING = "pending"
    LAUNCHED = "launched"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


TERMINAL = (StageStatus.PASSED, StageStatus.FAILED, StageStatus.SKIPPED)

KEY_MATERIALIZED = "materialized"
KEY_FAILED = "failed"
KEY_NOT_REACHED = "not_reached"


class StageGroupState(BaseModel):
    """One partition group's progress."""
    model_config = ConfigDict(extra="ignore")

    group_key: str
    partition_key: Optional[str] = None
    selection: List[List[str]]
    depends_on: List[str] = Field(default_factory=list)
    status: StageStatus = Field(default=StageStatus.PENDING)
    run_id: Optional[str] = None
    error: Optional[str] = None
    attempts: int = 0
    last_updated: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    key_outcomes: Dict[str, str] = Field(default_factory=dict)
    """``"/"``-joined asset key -> materialized | failed | not_reached,
    from the last finished run of this group."""


class StageState(BaseModel):
    """The full per-(qual_id, side) stage state file."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION)
    qual_id: str
    side: str
    started_at: datetime
    updated_at: datetime
    selection_hash: str
    plan: StagePlan
    groups: List[StageGroupState] = Field(default_factory=list)

    @property
    def passed(self) -> bool:
        """True only when there is something staged and all of it passed."""
        return bool(self.groups) and all(
            g.status == StageStatus.PASSED for g in self.groups
        )


class StageRecord(BaseModel):
    """One group run, persisted immutably under ``stage/runs/<run_id>.json``."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION)
    qual_id: str
    side: str
    run_id: str
    group_key: str
    partition_key: Optional[str] = None
    status: str
    selection: List[List[str]]
    """What was actually launched (wider than the planned group's
    selection when a non-subsettable multi_asset forced a retry)."""
    selection_hash: str
    key_outcomes: Dict[str, str] = Field(default_factory=dict)
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    event_count: int = 0


def default_local_stage_state_path(qual_id: str, side: str) -> Path:
    """``~/.dagtools/quals/<qual_id>/<side>-stage-state.json``."""
    home = Path(os.environ.get("DAGTOOLS_HOME") or (Path.home() / ".dagtools"))
    return home / "quals" / qual_id / f"{side}-stage-state.json"


def utcnow() -> datetime:
    """Single UTC clock so tests can monkeypatch this module only."""
    return datetime.now(tz=timezone.utc)


def new_state(qual_id: str, side: str, plan: StagePlan) -> StageState:
    now = utcnow()
    return StageState(
        qual_id=qual_id,
        side=side,
        started_at=now,
        updated_at=now,
        selection_hash=plan.selection_hash,
        plan=plan,
        groups=[
            StageGroupState(
                group_key=g.group_key,
                partition_key=g.partition_key,
                selection=[list(k) for k in g.selection],
                depends_on=list(g.depends_on),
            )
            for g in plan.groups
        ],
    )


def transition(
    group: StageGroupState,
    *,
    status: StageStatus,
    run_id: Optional[str] = None,
    error: Optional[str] = None,
    bump_attempts: bool = False,
    key_outcomes: Optional[Dict[str, str]] = None,
) -> StageGroupState:
    """Return a NEW group state with the transition applied."""
    now = utcnow()
    return group.model_copy(update={
        "status": status,
        "run_id": run_id if run_id is not None else group.run_id,
        "error": error,
        "last_updated": now,
        "finished_at": now if status in TERMINAL else None,
        "attempts": group.attempts + (1 if bump_attempts else 0),
        "key_outcomes": (
            key_outcomes if key_outcomes is not None else group.key_outcomes
        ),
    })

"""Staleness reporter for ``dagtools registry status``.

For every repo in ``inventory/``, reads ``latest.json`` and computes age
versus a configurable threshold. The result is a structured report the
CLI emits as JSON by default.

The recipe pass criterion for Part 1: every fleet repo's ``latest.json``
exists and is younger than the configured threshold (default 24h). Anything
else gets flagged.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field

from .client import InventoryRegistry, LatestPointer


class StalenessState(str, Enum):
    """Per-repo verdict from the staleness check."""

    FRESH = "fresh"           # latest.json present and younger than threshold
    STALE = "stale"           # present but older than threshold
    MISSING = "missing"       # repo prefix exists, but latest.json is absent
    UNREADABLE = "unreadable" # latest.json present but didn't parse


class RepoStatus(BaseModel):
    repo: str
    state: StalenessState
    pointer: Optional[LatestPointer] = None
    age_seconds: Optional[float] = None
    error: Optional[str] = None


class StatusReport(BaseModel):
    generated_at: datetime
    max_age_seconds: float
    repo_count: int
    fresh_count: int
    stale_count: int
    missing_count: int
    unreadable_count: int
    repos: List[RepoStatus] = Field(default_factory=list)


def compute_staleness(
    registry: InventoryRegistry,
    max_age: timedelta = timedelta(hours=24),
    now: Optional[datetime] = None,
) -> StatusReport:
    """Walk every repo under ``inventory/`` and report staleness vs. ``max_age``.

    ``now`` is injectable for tests; defaults to ``datetime.now(UTC)``.
    """
    now = now or datetime.now(tz=timezone.utc)
    threshold = max_age.total_seconds()

    repos = registry.list_repos()
    statuses: List[RepoStatus] = []

    for repo in repos:
        try:
            pointer = registry.read_latest_pointer(repo)
        except Exception as e:
            statuses.append(RepoStatus(
                repo=repo,
                state=StalenessState.UNREADABLE,
                error=f"{type(e).__name__}: {e}",
            ))
            continue

        if pointer is None:
            statuses.append(RepoStatus(repo=repo, state=StalenessState.MISSING))
            continue

        ts = pointer.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age = (now - ts).total_seconds()
        state = StalenessState.FRESH if age <= threshold else StalenessState.STALE
        statuses.append(RepoStatus(
            repo=repo, state=state, pointer=pointer, age_seconds=age
        ))

    fresh = sum(1 for s in statuses if s.state == StalenessState.FRESH)
    stale = sum(1 for s in statuses if s.state == StalenessState.STALE)
    missing = sum(1 for s in statuses if s.state == StalenessState.MISSING)
    unreadable = sum(1 for s in statuses if s.state == StalenessState.UNREADABLE)

    return StatusReport(
        generated_at=now,
        max_age_seconds=threshold,
        repo_count=len(statuses),
        fresh_count=fresh,
        stale_count=stale,
        missing_count=missing,
        unreadable_count=unreadable,
        repos=statuses,
    )

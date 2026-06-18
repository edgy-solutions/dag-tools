"""Per-probe state for ``dagtools qual probes run --side <side>``.

Mirrors the Q2 ``runs.state`` shape but lives in a SEPARATE slot:
``qualifications/<qual_id>/<side>/probes/state.json``. The separation
matters — a class can be both ``RUNNABLE`` (covered by a rep launched
in Q2) and ``SYNTHETIC_REQUIRED`` (covered by a probe launched here)
and the two paths can't be allowed to collide on (qual_id, side,
class_hash). Both shapes are tested independently; Q6 reads both.

Resumability: state is mirrored to the registry after every transition
(same discipline as ``runs.state.QualRunState``) so a desktop crash
mid-run loses at most one probe's mid-launch progress, and the next
invocation reconciles LAUNCHED entries via GraphQL run status rather
than relaunching.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


SCHEMA_VERSION = 1


class ProbeRepStatus(str, Enum):
    """Per-probe status during a Q5c side run.

    Same value set as the Q2 ``RepStatus`` so operators don't have to
    learn a second status grammar — but it's a SEPARATE type so a typo
    that crosses runnable-rep and probe-rep state paths fails type
    checks at the call site instead of silently working.
    """

    PENDING = "pending"
    LAUNCHED = "launched"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ProbeRepState(BaseModel):
    """One probe's state at a moment in time.

    Keyed by ``class_hash`` in :class:`ProbeRunState.probes` — exactly
    one probe per equivalence class, so no further disambiguator is
    needed (unlike the runnable side which has multiple reps per class).
    """
    model_config = ConfigDict(extra="ignore")

    class_hash: str
    module_name: str
    """The generated module name; used to derive the downstream asset
    key ``<module_name>_downstream`` we launch."""

    status: ProbeRepStatus = Field(default=ProbeRepStatus.PENDING)
    run_id: Optional[str] = None
    last_updated: Optional[datetime] = None
    error: Optional[str] = None
    attempts: int = 0


class ProbeRunState(BaseModel):
    """The full per-(qual_id, side) probe state file.

    Stored locally at ``~/.dagtools/quals/<qual_id>/<side>-probes-state.json``
    and mirrored to ``s3://.../qualifications/<qual_id>/<side>/probes/state.json``.
    """
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION)
    qual_id: str
    side: str
    started_at: datetime
    updated_at: datetime
    probes: Dict[str, ProbeRepState] = Field(default_factory=dict)


def default_local_probes_state_path(qual_id: str, side: str) -> Path:
    """``~/.dagtools/quals/<qual_id>/<side>-probes-state.json``.

    Sibling to ``<side>-state.json`` from the Q2 runner; deliberately
    not under ``probes/`` to keep the local layout flat for operators.
    """
    home = Path(os.environ.get("DAGTOOLS_HOME") or (Path.home() / ".dagtools"))
    return home / "quals" / qual_id / f"{side}-probes-state.json"


def utcnow() -> datetime:
    """Single UTC clock so tests can monkeypatch this module only."""
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# Selection helpers
# ---------------------------------------------------------------------------


def pending_or_resumable(state: ProbeRunState) -> List[ProbeRepState]:
    """Probes a fresh invocation should act on:
      * PENDING — never launched
      * LAUNCHED — desktop died after launch; reconcile via GraphQL
      * FAILED — caller decides whether to retry
    """
    return [
        p for p in state.probes.values()
        if p.status in (ProbeRepStatus.PENDING, ProbeRepStatus.LAUNCHED, ProbeRepStatus.FAILED)
    ]


def transition(
    rep: ProbeRepState,
    *,
    status: ProbeRepStatus,
    run_id: Optional[str] = None,
    error: Optional[str] = None,
    bump_attempts: bool = False,
) -> ProbeRepState:
    """Return a NEW ProbeRepState with the transition applied."""
    return rep.model_copy(update={
        "status": status,
        "run_id": run_id if run_id is not None else rep.run_id,
        "error": error,
        "last_updated": utcnow(),
        "attempts": rep.attempts + (1 if bump_attempts else 0),
    })

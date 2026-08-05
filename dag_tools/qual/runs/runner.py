"""``dagtools qual run --side <baseline|candidate>`` orchestrator.

End-to-end flow:

  1. Read the manifest + class matrix from the registry.
  2. Build (or load existing) :class:`QualRunState` for ``(qual_id, side)``.
  3. Pick the set of representatives to process:
       * RUNNABLE only (SYNTHETIC_REQUIRED + OBSERVE_ONLY are skipped here;
         Q5 handles synthetic separately, observe-only is a different
         comparison path).
       * PENDING — never launched, launch now.
       * LAUNCHED — desktop died; reconcile by polling the run_id.
       * FAILED — leave alone unless ``retry_failed=True`` is passed.
  4. For each entry: launch (or reconcile) → poll-to-terminal → fetch
     event log → build :class:`RunRecord` → persist to registry → update
     state.
  5. After all reps are terminal (or skipped), write the per-side summary
     and the final mirrored state.

Resumability: state is mirrored to the registry **after every transition**,
so a desktop crash mid-run loses at most one rep's mid-launch progress
(and even that is recoverable via GraphQL run status if the launch
itself succeeded).
"""
from __future__ import annotations

import json
import logging
from datetime import timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional

import yaml

from ..classes import (
    ClassMatrix,
    EquivalenceClass,
    Representative,
    Runnability,
)
from ..graphql import (
    DagsterGraphQLClient,
    DagsterGraphQLError,
    resolve_auth_token,
)
from ..qualify import QualificationManifest
from ..registry import InventoryRegistry
from .launcher import build_run_record, is_launchable, launch_representative
from .records import RunRecord
from .state import (
    QualRunState,
    RepState,
    RepStatus,
    default_local_state_path,
    pending_or_resumable,
    rep_id_for,
    transition,
    utcnow,
)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_side(
    qual_id: str,
    side: str,
    *,
    registry: InventoryRegistry,
    client_factory: Optional[Callable[[QualificationManifest], DagsterGraphQLClient]] = None,
    poll_interval_seconds: float = 5.0,
    poll_timeout_seconds: float = 1800.0,
    local_state_path: Optional[Path] = None,
    retry_failed: bool = False,
    only_class: Optional[str] = None,
    sleep: Optional[Callable[[float], None]] = None,
) -> "SideOutcome":
    """Run one side (baseline or candidate) of the qualification.

    ``client_factory`` is injectable so tests can mock the GraphQL client
    without touching httpx. Production callers leave it None to use the
    default factory which reads the manifest's deployment config.
    """
    manifest = _read_manifest(registry, qual_id)
    matrix = _read_class_matrix(registry, qual_id)
    local_state_path = local_state_path or default_local_state_path(qual_id, side)

    state = _load_or_init_state(registry, qual_id, side, matrix, local_state_path)

    factory = client_factory or _default_client_factory
    client = factory(manifest)
    try:
        _drive_state(
            client=client,
            state=state,
            matrix=matrix,
            manifest=manifest,
            registry=registry,
            qual_id=qual_id,
            side=side,
            local_state_path=local_state_path,
            poll_interval_seconds=poll_interval_seconds,
            poll_timeout_seconds=poll_timeout_seconds,
            retry_failed=retry_failed,
            only_class=only_class,
            sleep=sleep,
        )
    finally:
        client.close()

    summary = _write_summary(registry, qual_id, side, state)
    return SideOutcome(state=state, summary=summary)


# ---------------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------------


from pydantic import BaseModel, ConfigDict, Field  # noqa: E402  (after public funcs)


class SideSummary(BaseModel):
    """Aggregate of a side run — what Q6 reads to diff baseline vs candidate."""
    model_config = ConfigDict(extra="ignore")

    qual_id: str
    side: str
    rep_total: int
    pending: int
    launched: int
    passed: int
    failed: int
    skipped: int


class SideOutcome(BaseModel):
    """The end-state of one ``dagtools qual run --side`` invocation."""
    model_config = ConfigDict(extra="ignore")

    state: QualRunState
    summary: SideSummary


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------


def _read_manifest(registry: InventoryRegistry, qual_id: str) -> QualificationManifest:
    body = registry.read_qualification_manifest(qual_id)
    if body is None:
        raise FileNotFoundError(
            f"no qualification manifest at qual_id={qual_id!r}; "
            f"run `dagtools qual init --id {qual_id} ...` first"
        )
    return QualificationManifest.model_validate(yaml.safe_load(body))


def _read_class_matrix(registry: InventoryRegistry, qual_id: str) -> ClassMatrix:
    body = registry.read_qualification_classes_json(qual_id)
    if body is None:
        raise FileNotFoundError(
            f"no equivalence-class matrix for qual_id={qual_id!r}; "
            f"run `dagtools qual classes --id {qual_id}` first"
        )
    return ClassMatrix.model_validate_json(body)


def _load_or_init_state(
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    matrix: ClassMatrix,
    local_state_path: Path,
) -> QualRunState:
    """Prefer a registry-mirrored state file (so a different desktop can
    resume), then fall back to the local file (operator-local recovery
    from registry transient outage), then synthesize a fresh state from
    the class matrix."""
    registry_body = registry.read_side_state(qual_id, side)
    if registry_body:
        state = QualRunState.model_validate_json(registry_body)
        _backfill_from_matrix(state, matrix)
        return state

    if local_state_path.exists():
        state = QualRunState.model_validate_json(local_state_path.read_bytes())
        _backfill_from_matrix(state, matrix)
        return state

    return _new_state(qual_id, side, matrix)


def _new_state(qual_id: str, side: str, matrix: ClassMatrix) -> QualRunState:
    now = utcnow()
    reps: Dict[str, RepState] = {}
    for cls in matrix.classes:
        for rep in cls.representatives:
            rid = rep_id_for(cls.class_hash, rep.asset_key)
            reps[rid] = RepState(
                rep_id=rid,
                class_hash=cls.class_hash,
                asset_key=list(rep.asset_key),
                repo=rep.repo,
                git_sha=rep.git_sha,
                runnability=rep.runnability.value,
                status=RepStatus.PENDING,
                last_updated=now,
            )
    return QualRunState(
        qual_id=qual_id,
        side=side,
        started_at=now,
        updated_at=now,
        reps=reps,
    )


def _backfill_from_matrix(state: QualRunState, matrix: ClassMatrix) -> None:
    """Add any newly-introduced (e.g. via re-run of classes with
    --allow-overwrite) representatives to the existing state as PENDING.
    Existing entries are NOT touched."""
    for cls in matrix.classes:
        for rep in cls.representatives:
            rid = rep_id_for(cls.class_hash, rep.asset_key)
            if rid in state.reps:
                continue
            state.reps[rid] = RepState(
                rep_id=rid,
                class_hash=cls.class_hash,
                asset_key=list(rep.asset_key),
                repo=rep.repo,
                git_sha=rep.git_sha,
                runnability=rep.runnability.value,
                status=RepStatus.PENDING,
                last_updated=utcnow(),
            )


def _save_state(
    state: QualRunState,
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    local_path: Path,
) -> None:
    state.updated_at = utcnow()
    body = state.model_dump_json().encode("utf-8")
    # Local first — operator-fast feedback even if S3 is flaky.
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(body)
    try:
        registry.put_side_state(qual_id, side, body)
    except Exception as e:
        logger.warning("_save_state: registry mirror failed (%s); local copy is good", e)


def _drive_state(
    *,
    client: DagsterGraphQLClient,
    state: QualRunState,
    matrix: ClassMatrix,
    manifest: QualificationManifest,
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    local_state_path: Path,
    poll_interval_seconds: float,
    poll_timeout_seconds: float,
    retry_failed: bool,
    only_class: Optional[str],
    sleep: Optional[Callable[[float], None]],
) -> None:
    """Inner loop: walk the reps, launch / reconcile / collect."""
    rep_lookup = _build_rep_lookup(matrix)

    # Fetched ONCE per side, not per representative: it is a whole-
    # deployment snapshot and asking again per launch would add a round
    # trip for every rep while telling us nothing new. Empty dict on
    # failure — plan_asset_selection degrades to the plain single-key
    # selection rather than blocking the run.
    launch_info = client.get_asset_launch_info()
    if not launch_info:
        logger.warning(
            "no asset launch info from the deployment; partitioned assets "
            "and multi_asset siblings may fail to launch"
        )

    for rep_state in pending_or_resumable(state):
        if only_class and rep_state.class_hash != only_class:
            continue
        if rep_state.runnability != Runnability.RUNNABLE.value:
            # Synthetic / observe paths handled separately (Q5).
            rep_state = transition(rep_state, status=RepStatus.SKIPPED,
                                   error=f"non-runnable: {rep_state.runnability}")
            state.reps[rep_state.rep_id] = rep_state
            _save_state(state, registry, qual_id, side, local_state_path)
            continue
        if rep_state.status == RepStatus.FAILED and not retry_failed:
            continue

        rep = rep_lookup.get(rep_state.rep_id)
        if rep is None:
            # Defensive: class matrix lost track of this rep. Mark and move on.
            rep_state = transition(
                rep_state, status=RepStatus.SKIPPED,
                error="representative not present in current class matrix",
            )
            state.reps[rep_state.rep_id] = rep_state
            _save_state(state, registry, qual_id, side, local_state_path)
            continue

        # The deployment is the authority on whether a key can be launched
        # at all. Checked here rather than only in Q1 so an inventory
        # published before `is_executable` existed still skips its external
        # assets instead of accruing one launch failure per source table.
        launchable, why = is_launchable(list(rep.asset_key), launch_info)
        if not launchable:
            logger.info(
                "skipping %s: %s", "/".join(rep.asset_key), why,
            )
            rep_state = transition(rep_state, status=RepStatus.SKIPPED, error=why)
            state.reps[rep_state.rep_id] = rep_state
            _save_state(state, registry, qual_id, side, local_state_path)
            continue

        # --- LAUNCHED reconciliation: don't relaunch, poll the existing run_id.
        if rep_state.status == RepStatus.LAUNCHED and rep_state.run_id:
            _reconcile_or_finalize(
                client=client, state=state, rep_state=rep_state, rep=rep,
                registry=registry, manifest=manifest,
                qual_id=qual_id, side=side, local_state_path=local_state_path,
                poll_interval_seconds=poll_interval_seconds,
                poll_timeout_seconds=poll_timeout_seconds,
                sleep=sleep,
            )
            continue

        # --- PENDING or FAILED-with-retry: launch fresh.
        try:
            run_id = launch_representative(
                client, rep,
                qual_id=qual_id, side=side, manifest=manifest,
                location_name=manifest.deployment.location_name,
                job_name=manifest.deployment.job_name,
                launch_info=launch_info,
            )
        except DagsterGraphQLError as e:
            rep_state = transition(
                rep_state, status=RepStatus.FAILED,
                error=f"launch failed: {e}", bump_attempts=True,
            )
            state.reps[rep_state.rep_id] = rep_state
            _save_state(state, registry, qual_id, side, local_state_path)
            continue

        rep_state = transition(
            rep_state, status=RepStatus.LAUNCHED,
            run_id=run_id, bump_attempts=True,
        )
        state.reps[rep_state.rep_id] = rep_state
        _save_state(state, registry, qual_id, side, local_state_path)

        _reconcile_or_finalize(
            client=client, state=state, rep_state=rep_state, rep=rep,
            registry=registry, manifest=manifest,
            qual_id=qual_id, side=side, local_state_path=local_state_path,
            poll_interval_seconds=poll_interval_seconds,
            poll_timeout_seconds=poll_timeout_seconds,
            sleep=sleep,
        )


def _reconcile_or_finalize(
    *,
    client: DagsterGraphQLClient,
    state: QualRunState,
    rep_state: RepState,
    rep: Representative,
    registry: InventoryRegistry,
    manifest: QualificationManifest,
    qual_id: str,
    side: str,
    local_state_path: Path,
    poll_interval_seconds: float,
    poll_timeout_seconds: float,
    sleep: Optional[Callable[[float], None]],
) -> None:
    """Poll a launched rep until terminal, then persist the record + state."""
    try:
        info = client.poll_to_completion(
            rep_state.run_id,
            interval_seconds=poll_interval_seconds,
            timeout_seconds=poll_timeout_seconds,
            sleep=sleep,
        )
        events = client.get_event_log(rep_state.run_id)
    except DagsterGraphQLError as e:
        rep_state = transition(
            rep_state, status=RepStatus.FAILED,
            error=f"poll/event-log failed: {e}",
        )
        state.reps[rep_state.rep_id] = rep_state
        _save_state(state, registry, qual_id, side, local_state_path)
        return

    record = build_run_record(
        qual_id=qual_id, side=side, class_hash=rep_state.class_hash,
        rep=rep, run_status=info, events=events,
    )
    registry.put_run_record(
        qual_id=qual_id, side=side,
        class_hash=rep_state.class_hash, run_id=info.run_id,
        body=record.model_dump_json().encode("utf-8"),
    )
    new_status = RepStatus.PASSED if info.succeeded else RepStatus.FAILED
    rep_state = transition(
        rep_state, status=new_status,
        run_id=info.run_id,
        error=record.error if not info.succeeded else None,
    )
    state.reps[rep_state.rep_id] = rep_state
    _save_state(state, registry, qual_id, side, local_state_path)


def _build_rep_lookup(matrix: ClassMatrix) -> Dict[str, Representative]:
    out: Dict[str, Representative] = {}
    for cls in matrix.classes:
        for rep in cls.representatives:
            out[rep_id_for(cls.class_hash, rep.asset_key)] = rep
    return out


def _write_summary(
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    state: QualRunState,
) -> SideSummary:
    counts = {s: 0 for s in RepStatus}
    for rep in state.reps.values():
        counts[rep.status] = counts.get(rep.status, 0) + 1
    summary = SideSummary(
        qual_id=qual_id,
        side=side,
        rep_total=len(state.reps),
        pending=counts[RepStatus.PENDING],
        launched=counts[RepStatus.LAUNCHED],
        passed=counts[RepStatus.PASSED],
        failed=counts[RepStatus.FAILED],
        skipped=counts[RepStatus.SKIPPED],
    )
    body = summary.model_dump_json().encode("utf-8")
    try:
        registry.put_side_summary(qual_id, side, body, allow_overwrite=True)
    except Exception as e:
        logger.warning("_write_summary: registry write failed: %s", e)
    return summary


def _default_client_factory(manifest: QualificationManifest) -> DagsterGraphQLClient:
    """Build a client from the manifest's deployment config.

    The manifest is the contract for *where to connect* and *how to
    authenticate*. The CLI delegates here so tests can swap the whole
    client out with a mock by passing ``client_factory=...``.
    """
    if not manifest.deployment.graphql_url:
        raise RuntimeError(
            "manifest.deployment.graphql_url is not set; pass --graphql-url "
            "to `dagtools qual init` or update the manifest"
        )
    token = resolve_auth_token(manifest.deployment.auth)
    return DagsterGraphQLClient(
        endpoint_url=manifest.deployment.graphql_url,
        auth_token=token,
    )

"""``dagtools qual stage --side <baseline|candidate>`` orchestrator.

Materializes the computed upstreams Q2's representatives will load as
inputs, in runs of their own. One run per partition group (usually exactly
one). A run cannot mix partition definitions: depending on the key the
deployment either rejects the launch or fails partway after partially
materializing, so groups are launched separately, in dependency order.

Invariants shared with Q2 (``runs.runner``): state is saved after every
transition; PASSED is sacred; LAUNCHED reconciles by polling, never by
relaunching. Unlike Q2, an empty ``launch_info`` refuses to launch.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, ConfigDict, Field

from ..graphql import (
    DagsterGraphQLClient,
    DagsterGraphQLError,
    DagsterSubsetNotSupportedError,
    EventLogEntry,
    RunStatusInfo,
)
from ..qualify import QualificationManifest
from ..registry import ImmutableKeyExists, InventoryRegistry
from ..runs.launcher import (
    DEFAULT_JOB_NAME,
    DEFAULT_REPOSITORY_NAME,
    PARTITION_NAME_TAG,
    expand_to_op_siblings,
)
from ..runs.runner import (
    _default_client_factory,
    _read_class_matrix,
    _read_manifest,
)
from ..runs.staging import (
    StagingOverridesNotResolved,
    merge_run_config,
    resolve_staging_overrides,
    storage_fetch,
)
from .plan import (
    StageGroupPlan,
    StagePlan,
    build_stage_plan,
    reps_ready,
)
from .state import (
    KEY_FAILED,
    KEY_MATERIALIZED,
    KEY_NOT_REACHED,
    StageGroupState,
    StageRecord,
    StageState,
    StageStatus,
    default_local_stage_state_path,
    new_state,
    transition,
    utcnow,
)


logger = logging.getLogger(__name__)


class StageTooLarge(RuntimeError):
    """The selection exceeds ``--max-keys``."""


class StageOutcome(BaseModel):
    """What one ``dagtools qual stage`` invocation reports."""
    model_config = ConfigDict(extra="ignore")

    qual_id: str
    side: str
    plan_only: bool = False
    plan: StagePlan
    state: Optional[StageState] = None
    status: str = "planned"
    """planned | nothing_to_stage | passed | failed | incomplete"""
    reps_ready: List[str] = Field(default_factory=list)
    reps_not_ready: List[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_stage(
    qual_id: str,
    side: str,
    *,
    registry: InventoryRegistry,
    client_factory: Optional[Callable[[QualificationManifest], DagsterGraphQLClient]] = None,
    plan_only: bool = False,
    force: bool = False,
    max_keys: Optional[int] = None,
    poll_interval_seconds: float = 5.0,
    poll_timeout_seconds: float = 1800.0,
    local_state_path: Optional[Path] = None,
    sleep: Optional[Callable[[float], None]] = None,
) -> StageOutcome:
    manifest = _read_manifest(registry, qual_id)
    matrix = _read_class_matrix(registry, qual_id)
    local_state_path = local_state_path or default_local_stage_state_path(qual_id, side)

    client = (client_factory or _default_client_factory)(manifest)
    try:
        # Once per side: a whole-deployment snapshot, as in Q2.
        launch_info = client.get_asset_launch_info()
        plan = build_stage_plan(matrix, launch_info)  # raises on empty info

        if plan_only:
            ready, not_ready = reps_ready(plan, set())
            return StageOutcome(
                qual_id=qual_id, side=side, plan_only=True, plan=plan,
                reps_ready=ready, reps_not_ready=not_ready,
            )

        if max_keys is not None and plan.key_count > max_keys:
            raise StageTooLarge(
                f"selection has {plan.key_count} keys, over --max-keys "
                f"{max_keys}; nothing was launched"
            )
        if not plan.groups:
            ready, not_ready = reps_ready(plan, set())
            return StageOutcome(
                qual_id=qual_id, side=side, plan=plan, status="nothing_to_stage",
                reps_ready=ready, reps_not_ready=not_ready,
            )

        # Resolved ONCE per side and never degraded: staged outputs must land
        # where Q2 will read them, or Q2 never finds them.
        staging_overrides = resolve_staging_overrides(
            manifest.staging_overrides, fetch=storage_fetch(registry.storage)
        )

        state = _load_or_init_state(
            registry, qual_id, side, plan, local_state_path, force=force
        )
        _drive(
            client=client, state=state, manifest=manifest,
            registry=registry, qual_id=qual_id, side=side,
            launch_info=launch_info, staging_overrides=staging_overrides,
            local_state_path=local_state_path,
            poll_interval_seconds=poll_interval_seconds,
            poll_timeout_seconds=poll_timeout_seconds, sleep=sleep,
        )
    finally:
        client.close()

    ready, not_ready = reps_ready(plan, _materialized_keys(state))
    return StageOutcome(
        qual_id=qual_id, side=side, plan=plan, state=state,
        status=_overall_status(state),
        reps_ready=ready, reps_not_ready=not_ready,
    )


# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------


def launch_stage_group(
    client: DagsterGraphQLClient,
    group: StageGroupPlan,
    *,
    qual_id: str,
    side: str,
    selection_hash: str,
    manifest: QualificationManifest,
    launch_info: Dict[str, Dict[str, Any]],
    staging_overrides: Optional[Dict[str, Any]] = None,
) -> Tuple[str, List[List[str]]]:
    """Submit one group's launch. Returns ``(run_id, launched_selection)``.

    No ``dagtools/class_hash`` tag: a stage run must never be mistaken for a
    representative run on the deployment.
    """
    if manifest.staging_overrides and staging_overrides is None:
        raise StagingOverridesNotResolved(
            f"manifest declares staging_overrides={manifest.staging_overrides!r} "
            "but no resolved document was passed to launch_stage_group; "
            "call resolve_staging_overrides() first"
        )
    run_config = merge_run_config(overrides=staging_overrides, explicit=None)

    tags = {
        "dagtools/qual": qual_id,
        "dagtools/side": side,
        "dagtools/purpose": "stage",
        "dagtools/stage_selection_hash": selection_hash,
        "dagtools/stage_group": group.group_key,
    }
    if group.partition_key is not None:
        tags[PARTITION_NAME_TAG] = group.partition_key

    def _launch(sel: List[List[str]]) -> str:
        return client.launch_asset_run(
            location_name=manifest.deployment.location_name or "default",
            repository_name=DEFAULT_REPOSITORY_NAME,
            job_name=manifest.deployment.job_name or DEFAULT_JOB_NAME,
            asset_selection=sel,
            run_config=run_config,
            tags=tags,
        )

    selection = [list(k) for k in group.selection]
    try:
        return _launch(selection), selection
    except DagsterSubsetNotSupportedError:
        # A non-subsettable multi_asset refused a partial selection. Widen
        # to op-mates and retry this group once. Op-mates share a
        # partitions definition, so no key changes group. Their own
        # computed upstreams are NOT added: re-planning here would
        # invalidate the selection hash.
        widened = sorted({
            tuple(k) for key in selection
            for k in expand_to_op_siblings(key, launch_info)
        })
        widened_list = [list(k) for k in widened]
        if len(widened_list) <= len(selection):
            raise
        logger.info(
            "qual stage: group %s refused a partial selection; retrying with "
            "%d op-mates", group.group_key, len(widened_list) - len(selection),
        )
        return _launch(widened_list), widened_list


# ---------------------------------------------------------------------------
# Record assembly
# ---------------------------------------------------------------------------


def key_outcomes_from_events(
    selection: List[List[str]],
    events: List[EventLogEntry],
    launch_info: Dict[str, Dict[str, Any]],
) -> Dict[str, str]:
    """Per-key outcome from the event log.

    ``materialized`` needs a real materialization event. ``failed`` is a key
    whose op had a step failure; anything else the run never got to.
    """
    done: Set[str] = set()
    failed_steps: List[str] = []
    for ev in events:
        try:
            if ev.event_type == "MaterializationEvent" and ev.asset_key:
                done.add("/".join(ev.asset_key))
            elif ev.event_type == "ExecutionStepFailureEvent" and ev.step_key:
                failed_steps.append(ev.step_key)
        except Exception as e:  # noqa: BLE001 -- soft-fail per event
            logger.warning("stage: skipping bad event: %s", e)

    out: Dict[str, str] = {}
    for key in selection:
        name = "/".join(key)
        if name in done:
            out[name] = KEY_MATERIALIZED
            continue
        ops = (launch_info.get(name) or {}).get("op_names") or []
        hit = any(
            s == op or s.startswith(op + "[") or s.startswith(op + ".")
            for s in failed_steps for op in ops
        )
        out[name] = KEY_FAILED if hit else KEY_NOT_REACHED
    return out


def build_stage_record(
    *,
    qual_id: str,
    side: str,
    group: StageGroupState,
    selection: List[List[str]],
    selection_hash: str,
    run_status: RunStatusInfo,
    events: List[EventLogEntry],
    key_outcomes: Dict[str, str],
) -> StageRecord:
    error = None
    if not run_status.succeeded:
        for ev in reversed(events):
            if ev.event_type == "ExecutionStepFailureEvent" and ev.message:
                error = ev.message
                break
        error = error or f"run finished {run_status.status}"
    return StageRecord(
        qual_id=qual_id, side=side, run_id=run_status.run_id,
        group_key=group.group_key, partition_key=group.partition_key,
        status=run_status.status, selection=selection,
        selection_hash=selection_hash, key_outcomes=key_outcomes, error=error,
        started_at=_epoch(run_status.start_time),
        ended_at=_epoch(run_status.end_time), event_count=len(events),
    )


def _epoch(ts: Optional[float]) -> Optional[datetime]:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------


def _load_or_init_state(
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    plan: StagePlan,
    local_path: Path,
    *,
    force: bool,
) -> StageState:
    """Resume a state for the SAME selection hash; anything else starts fresh.

    A different hash means the matrix (or the deployment's view of it)
    changed, so an old PASSED says nothing about what is staged now.
    """
    existing: Optional[StageState] = None
    body = registry.read_stage_state(qual_id, side)
    if body:
        existing = StageState.model_validate_json(body)
    elif local_path.exists():
        existing = StageState.model_validate_json(local_path.read_bytes())

    if existing and not force and existing.selection_hash == plan.selection_hash:
        return existing
    return new_state(qual_id, side, plan)


def _save_state(
    state: StageState,
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    local_path: Path,
) -> None:
    state.updated_at = utcnow()
    body = state.model_dump_json().encode("utf-8")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(body)
    try:
        registry.put_stage_state(qual_id, side, body)
    except Exception as e:  # noqa: BLE001
        logger.warning("stage: registry mirror failed (%s); local copy is good", e)


def _failed_roots(state: StageState, group: StageGroupState) -> List[str]:
    """The FAILED groups a group transitively depends on."""
    by_key = {g.group_key: g for g in state.groups}
    roots: Set[str] = set()
    seen: Set[str] = set()
    stack = list(group.depends_on)
    while stack:
        k = stack.pop()
        if k in seen:
            continue
        seen.add(k)
        dep = by_key.get(k)
        if dep is None:
            continue
        if dep.status == StageStatus.FAILED:
            roots.add(k)
        elif dep.status != StageStatus.PASSED:
            stack.extend(dep.depends_on)
    return sorted(roots)


def _drive(
    *,
    client: DagsterGraphQLClient,
    state: StageState,
    manifest: QualificationManifest,
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    launch_info: Dict[str, Dict[str, Any]],
    staging_overrides: Dict[str, Any],
    local_state_path: Path,
    poll_interval_seconds: float,
    poll_timeout_seconds: float,
    sleep: Optional[Callable[[float], None]],
) -> None:
    def put(idx: int, group: StageGroupState) -> None:
        state.groups[idx] = group
        _save_state(state, registry, qual_id, side, local_state_path)

    # The plan orders the groups; state.groups keeps that order.
    for idx in range(len(state.groups)):
        group = state.groups[idx]
        if group.status == StageStatus.PASSED:
            continue  # sacred

        statuses = {g.group_key: g.status for g in state.groups}
        blockers = [d for d in group.depends_on
                    if statuses.get(d) != StageStatus.PASSED]
        if blockers:
            roots = _failed_roots(state, group)
            reason = (
                "skipped: depends on failed group(s) " + ", ".join(roots)
                if roots else
                "skipped: depends on unfinished group(s) " + ", ".join(sorted(blockers))
            )
            put(idx, transition(group, status=StageStatus.SKIPPED, error=reason))
            continue

        if group.status == StageStatus.LAUNCHED and group.run_id:
            # Reconcile the existing run; never relaunch.
            launched_selection = group.selection
        else:
            try:
                run_id, launched_selection = launch_stage_group(
                    client,
                    StageGroupPlan(
                        group_key=group.group_key,
                        partition_key=group.partition_key,
                        selection=group.selection,
                        depends_on=group.depends_on,
                    ),
                    qual_id=qual_id, side=side,
                    selection_hash=state.selection_hash, manifest=manifest,
                    launch_info=launch_info, staging_overrides=staging_overrides,
                )
            except DagsterGraphQLError as e:
                put(idx, transition(
                    group, status=StageStatus.FAILED,
                    error=f"launch failed: {e}", bump_attempts=True,
                ))
                continue
            group = transition(
                group, status=StageStatus.LAUNCHED, run_id=run_id,
                bump_attempts=True,
            )
            put(idx, group)

        try:
            info = client.poll_to_completion(
                group.run_id, interval_seconds=poll_interval_seconds,
                timeout_seconds=poll_timeout_seconds, sleep=sleep,
            )
            events = client.get_event_log(group.run_id)
        except DagsterGraphQLError as e:
            # The run may still be going: stay LAUNCHED so a re-invocation
            # reconciles it rather than starting a concurrent duplicate.
            put(idx, transition(
                group, status=StageStatus.LAUNCHED,
                error=f"poll/event-log failed, will reconcile: {e}",
            ))
            continue

        outcomes = key_outcomes_from_events(launched_selection, events, launch_info)
        record = build_stage_record(
            qual_id=qual_id, side=side, group=group,
            selection=launched_selection, selection_hash=state.selection_hash,
            run_status=info, events=events, key_outcomes=outcomes,
        )
        try:
            registry.put_stage_run_record(
                qual_id, side, info.run_id, record.model_dump_json().encode("utf-8"),
            )
        except ImmutableKeyExists:
            # Banked by an earlier pass that stopped before saving state.
            logger.info("stage: record for %s already present", info.run_id)
        put(idx, transition(
            group,
            status=StageStatus.PASSED if info.succeeded else StageStatus.FAILED,
            run_id=info.run_id,
            error=None if info.succeeded else record.error,
            key_outcomes=outcomes,
        ))


def _materialized_keys(state: StageState) -> Set[Tuple[str, ...]]:
    return {
        tuple(name.split("/"))
        for g in state.groups
        for name, outcome in g.key_outcomes.items()
        if outcome == KEY_MATERIALIZED
    }


def _overall_status(state: StageState) -> str:
    if state.passed:
        return "passed"
    if any(g.status in (StageStatus.PENDING, StageStatus.LAUNCHED)
           for g in state.groups):
        return "incomplete"
    return "failed"

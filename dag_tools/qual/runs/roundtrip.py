"""Q2 item 4: the IO round-trip pass of ``dagtools qual run --side <s>``.

After the representative pass, each class's generated round-trip probe (in
the ``dag-tools-probes`` location) loads the output the side's Q2 run just
wrote, through the class's real IO manager and Dagster's own input-loading
path. See ``docs/plans/q2-roundtrip-probe.md``.

Per probe, in order:

  1. Gate on the representative: not PASSED on this side -> SKIPPED.
  2. Freshness (D8): the rep's latest materialization must be the Q2 run.
     Otherwise the probe could load another run's output (the other side's,
     on shared storage) and pass on it -> SKIPPED.
  3. Register the partition the Q2 run actually used (D3) on the dynamic
     stand-in. A key that already exists is success.
  4. Launch the probe asset ALONE on ``dag-tools-probes``.
  5. Poll, build a :class:`RunRecord`, persist, update state.

Invariants shared with the Q2 and probe runners: PASSED is sacred, LAUNCHED
reconciles by polling (never relaunches), only ``DagsterRunNotFoundError``
makes a run FAILED, and state is saved after every transition.

``RoundTripStatus`` / ``RoundTripState`` are deliberately distinct from
``RepStatus`` / ``RepState`` and ``ProbeRepStatus`` / ``ProbeRepState``:
this slot must never be wired to either by accident.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from ..graphql import (
    DagsterGraphQLClient,
    DagsterGraphQLError,
    DagsterRunNotFoundError,
)
from ..qualify import QualificationManifest
from ..registry import InventoryRegistry
from ..synthetic import ProbeManifest
from ..synthetic.location import (
    PROBES_JOB_NAME,
    PROBES_LOCATION_NAME,
    PROBES_REPOSITORY_NAME,
)
from ..synthetic.schema import RoundTripProbe
from .launcher import build_run_record
from .records import RunRecord
from .staging import (
    StagingOverridesNotResolved,
    merge_run_config,
    resolve_staging_overrides,
    storage_fetch,
)
from .state import QualRunState, RepStatus


logger = logging.getLogger(__name__)


PARTITION_NAME_TAG = "dagster/partition"

SKIP_REP_NOT_PASSED = "representative not passed"
SKIP_REMATERIALIZED = "upstream rematerialized since the Q2 run"

SCHEMA_VERSION = 1


# ---------------------------------------------------------------------------
# State (its own slot: <side>/roundtrip/state.json)
# ---------------------------------------------------------------------------


class RoundTripStatus(str, Enum):
    PENDING = "pending"
    LAUNCHED = "launched"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


class RoundTripState(BaseModel):
    """One class's round-trip state at a moment in time."""
    model_config = ConfigDict(extra="ignore")

    class_hash: str
    rep_id: str
    status: RoundTripStatus = Field(default=RoundTripStatus.PENDING)
    run_id: Optional[str] = None
    partition_key: Optional[str] = None
    rep_run_id: Optional[str] = None
    """The Q2 run whose output this probe loads."""
    error: Optional[str] = None
    attempts: int = 0
    last_updated: Optional[datetime] = None


class RoundTripRunState(BaseModel):
    """The per-(qual_id, side) round-trip state file."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION)
    qual_id: str
    side: str
    started_at: datetime
    updated_at: datetime
    probes: Dict[str, RoundTripState] = Field(default_factory=dict)


def default_local_roundtrip_state_path(qual_id: str, side: str) -> Path:
    """``~/.dagtools/quals/<qual_id>/<side>-roundtrip-state.json``."""
    home = Path(os.environ.get("DAGTOOLS_HOME") or (Path.home() / ".dagtools"))
    return home / "quals" / qual_id / f"{side}-roundtrip-state.json"


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def transition(
    probe: RoundTripState,
    *,
    status: RoundTripStatus,
    run_id: Optional[str] = None,
    partition_key: Optional[str] = None,
    rep_run_id: Optional[str] = None,
    error: Optional[str] = None,
    bump_attempts: bool = False,
) -> RoundTripState:
    """Return a NEW state with the transition applied."""
    return probe.model_copy(update={
        "status": status,
        "run_id": run_id if run_id is not None else probe.run_id,
        "partition_key": (
            partition_key if partition_key is not None else probe.partition_key
        ),
        "rep_run_id": rep_run_id if rep_run_id is not None else probe.rep_run_id,
        "error": error,
        "last_updated": utcnow(),
        "attempts": probe.attempts + (1 if bump_attempts else 0),
    })


# ---------------------------------------------------------------------------
# Outcome
# ---------------------------------------------------------------------------


class RoundTripSummary(BaseModel):
    model_config = ConfigDict(extra="ignore")

    qual_id: str
    side: str
    probe_total: int = 0
    pending: int = 0
    launched: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0


class RoundTripOutcome(BaseModel):
    model_config = ConfigDict(extra="ignore")

    recorded: bool
    """False when the pass recorded nothing (no round-trip probes in the
    manifest, or ``dag-tools-probes`` absent from the workspace)."""
    note: Optional[str] = None
    state: Optional[RoundTripRunState] = None
    summary: Optional[RoundTripSummary] = None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_roundtrip_side(
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
) -> RoundTripOutcome:
    """Run the round-trip pass for one side. Never raises for an absent
    probe bundle or an absent probes location: it records nothing and says so."""
    probe_body = registry.read_probe_manifest(qual_id)
    if probe_body is None:
        return RoundTripOutcome(
            recorded=False,
            note="round-trip pass recorded nothing: no probe manifest "
                 "(run `dagtools qual synthetic` first)",
        )
    probe_manifest = ProbeManifest.model_validate_json(probe_body)
    if not probe_manifest.roundtrip_probes:
        return RoundTripOutcome(
            recorded=False,
            note="round-trip pass recorded nothing: the probe manifest has no "
                 "round-trip probes (re-run `dagtools qual synthetic`)",
        )

    manifest = _read_manifest(registry, qual_id)
    if client_factory is None:
        # Looked up at call time so one patch of the Q2 runner's default
        # factory covers both passes.
        from . import runner as q2_runner

        client_factory = q2_runner._default_client_factory
    client = client_factory(manifest)
    try:
        try:
            locations = client.get_code_locations()
        except DagsterGraphQLError as e:
            return RoundTripOutcome(
                recorded=False,
                note=f"round-trip pass recorded nothing: workspace query failed: {e}",
            )
        if not any(loc.name == PROBES_LOCATION_NAME for loc in locations):
            return RoundTripOutcome(
                recorded=False,
                note=f"round-trip pass recorded nothing: code location "
                     f"{PROBES_LOCATION_NAME!r} is absent from the workspace",
            )

        local_state_path = local_state_path or default_local_roundtrip_state_path(
            qual_id, side
        )
        state = _load_or_init_state(
            registry, qual_id, side, probe_manifest, local_state_path
        )
        rep_state = _read_q2_state(registry, qual_id, side)
        _drive_state(
            client=client, state=state, probe_manifest=probe_manifest,
            q2_state=rep_state, manifest=manifest, registry=registry,
            qual_id=qual_id, side=side, local_state_path=local_state_path,
            poll_interval_seconds=poll_interval_seconds,
            poll_timeout_seconds=poll_timeout_seconds,
            retry_failed=retry_failed, only_class=only_class, sleep=sleep,
        )
    finally:
        client.close()

    return RoundTripOutcome(
        recorded=True, state=state, summary=_summarize(qual_id, side, state),
    )


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _read_manifest(registry: InventoryRegistry, qual_id: str) -> QualificationManifest:
    import yaml

    body = registry.read_qualification_manifest(qual_id)
    if body is None:
        raise FileNotFoundError(f"no qualification manifest at qual_id={qual_id!r}")
    return QualificationManifest.model_validate(yaml.safe_load(body))


def _read_q2_state(
    registry: InventoryRegistry, qual_id: str, side: str
) -> Optional[QualRunState]:
    body = registry.read_side_state(qual_id, side)
    if not body:
        return None
    return QualRunState.model_validate_json(body)


def _new_probe_state(probe: RoundTripProbe) -> RoundTripState:
    return RoundTripState(
        class_hash=probe.class_hash, rep_id=probe.rep_id,
        status=RoundTripStatus.PENDING, last_updated=utcnow(),
    )


def _load_or_init_state(
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    probe_manifest: ProbeManifest,
    local_state_path: Path,
) -> RoundTripRunState:
    body = registry.read_roundtrip_state(qual_id, side)
    state: Optional[RoundTripRunState] = None
    if body:
        state = RoundTripRunState.model_validate_json(body)
    elif local_state_path.exists():
        state = RoundTripRunState.model_validate_json(local_state_path.read_bytes())
    if state is None:
        now = utcnow()
        state = RoundTripRunState(
            qual_id=qual_id, side=side, started_at=now, updated_at=now,
        )
    for probe in probe_manifest.roundtrip_probes:
        state.probes.setdefault(probe.class_hash, _new_probe_state(probe))
    return state


def _save_state(
    state: RoundTripRunState,
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
        registry.put_roundtrip_state(qual_id, side, body)
    except Exception as e:  # noqa: BLE001
        logger.warning("_save_state: registry mirror failed (%s); local copy is good", e)


def _set(
    state: RoundTripRunState, probe_state: RoundTripState, ctx: "_Ctx", **kw: Any
) -> RoundTripState:
    new = transition(probe_state, **kw)
    state.probes[new.class_hash] = new
    _save_state(state, ctx.registry, ctx.qual_id, ctx.side, ctx.local_state_path)
    return new


class _Ctx:
    def __init__(self, **kw: Any):
        self.__dict__.update(kw)


def _drive_state(
    *,
    client: DagsterGraphQLClient,
    state: RoundTripRunState,
    probe_manifest: ProbeManifest,
    q2_state: Optional[QualRunState],
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
    ctx = _Ctx(
        client=client, registry=registry, qual_id=qual_id, side=side,
        local_state_path=local_state_path,
        poll_interval_seconds=poll_interval_seconds,
        poll_timeout_seconds=poll_timeout_seconds, sleep=sleep,
    )
    lookup = {p.class_hash: p for p in probe_manifest.roundtrip_probes}

    # Resolved ONCE per side, and fatal on failure: launching against
    # un-overridden resources would bank results describing the wrong
    # deployment (same rule as the Q2 and probe runners).
    staging_overrides = resolve_staging_overrides(
        manifest.staging_overrides, fetch=storage_fetch(registry.storage)
    )

    for class_hash in sorted(state.probes):
        if only_class and class_hash != only_class:
            continue
        probe_state = state.probes[class_hash]
        status = probe_state.status
        if status == RoundTripStatus.PASSED:
            continue  # sacred
        if status == RoundTripStatus.FAILED and not retry_failed:
            continue
        probe = lookup.get(class_hash)
        if probe is None:
            _set(state, probe_state, ctx, status=RoundTripStatus.SKIPPED,
                 error="probe not present in current probe manifest")
            continue

        if status == RoundTripStatus.LAUNCHED and probe_state.run_id:
            _reconcile(state, probe_state, probe, ctx)
            continue

        # PENDING, SKIPPED (re-evaluated: the reason may have cleared), or
        # FAILED with retry. Everything below can still end in SKIPPED.
        rep = (q2_state.reps.get(probe.rep_id) if q2_state else None)
        if rep is None or rep.status != RepStatus.PASSED or not rep.run_id:
            _set(state, probe_state, ctx, status=RoundTripStatus.SKIPPED,
                 error=SKIP_REP_NOT_PASSED)
            continue
        rep_run_id = rep.run_id

        try:
            partition_key = _partition_of_q2_run(
                client, registry, qual_id, side, probe, rep_run_id
            )
        except DagsterGraphQLError as e:
            _set(state, probe_state, ctx, status=RoundTripStatus.SKIPPED,
                 error=f"could not read the Q2 run's partition: {e}")
            continue
        if probe.partitions_def_name and partition_key is None:
            _set(state, probe_state, ctx, status=RoundTripStatus.SKIPPED,
                 error="the Q2 run's partition key could not be determined")
            continue

        try:
            latest = client.get_latest_materialization_run_id(
                list(probe.asset_key), partition_key
            )
        except DagsterGraphQLError as e:
            _set(state, probe_state, ctx, status=RoundTripStatus.SKIPPED,
                 error=f"freshness check failed: {e}")
            continue
        if latest != rep_run_id:
            _set(state, probe_state, ctx, status=RoundTripStatus.SKIPPED,
                 rep_run_id=rep_run_id, partition_key=partition_key,
                 error=SKIP_REMATERIALIZED)
            continue

        try:
            if probe.partitions_def_name and partition_key is not None:
                client.add_dynamic_partition(
                    location_name=PROBES_LOCATION_NAME,
                    repository_name=PROBES_REPOSITORY_NAME,
                    partitions_def_name=probe.partitions_def_name,
                    partition_key=partition_key,
                )
            run_id = _launch(
                client, probe, qual_id=qual_id, side=side, manifest=manifest,
                partition_key=partition_key, staging_overrides=staging_overrides,
            )
        except DagsterGraphQLError as e:
            _set(state, probe_state, ctx, status=RoundTripStatus.FAILED,
                 rep_run_id=rep_run_id, partition_key=partition_key,
                 error=f"launch failed: {e}", bump_attempts=True)
            continue

        probe_state = _set(
            state, probe_state, ctx, status=RoundTripStatus.LAUNCHED,
            run_id=run_id, rep_run_id=rep_run_id, partition_key=partition_key,
            bump_attempts=True,
        )
        _reconcile(state, probe_state, probe, ctx)


def _partition_of_q2_run(
    client: DagsterGraphQLClient,
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    probe: RoundTripProbe,
    rep_run_id: str,
) -> Optional[str]:
    """The partition the Q2 run actually used (D3): its record's
    ``partition_key``; for a record written before that field existed, the
    run's own ``dagster/partition`` tag. Never recomputed with
    ``last_partition_key`` -- a time-based partition list grows, so a
    recomputed key can name a partition the Q2 run never wrote."""
    if not probe.partitions_def_name:
        return None
    body = registry.read_run_record(qual_id, side, probe.class_hash, rep_run_id)
    if body:
        record = RunRecord.model_validate_json(body)
        if record.partition_key:
            return record.partition_key
    value = client.get_run_tags(rep_run_id).get(PARTITION_NAME_TAG)
    return value if isinstance(value, str) and value else None


def _launch(
    client: DagsterGraphQLClient,
    probe: RoundTripProbe,
    *,
    qual_id: str,
    side: str,
    manifest: QualificationManifest,
    partition_key: Optional[str],
    staging_overrides: Optional[Dict[str, Any]],
) -> str:
    """Launch the probe asset ALONE: its input is loaded from the IO manager,
    which is the point; selecting the representative too would rewrite it."""
    tags = {
        "dagtools/qual": qual_id,
        "dagtools/side": side,
        "dagtools/roundtrip": "true",
        "dagtools/class_hash": probe.class_hash,
    }
    if partition_key is not None:
        tags[PARTITION_NAME_TAG] = partition_key
    if manifest.staging_overrides and staging_overrides is None:
        raise StagingOverridesNotResolved(
            f"manifest declares staging_overrides={manifest.staging_overrides!r} "
            "but no resolved document reached the round-trip launcher"
        )
    run_config = merge_run_config(overrides=staging_overrides, explicit=None)
    return client.launch_asset_run(
        location_name=PROBES_LOCATION_NAME,
        repository_name=PROBES_REPOSITORY_NAME,
        job_name=PROBES_JOB_NAME,
        asset_selection=[list(probe.probe_asset_key)],
        run_config=run_config,
        tags=tags,
    )


def _reconcile(
    state: RoundTripRunState,
    probe_state: RoundTripState,
    probe: RoundTripProbe,
    ctx: "_Ctx",
) -> None:
    client = ctx.client
    try:
        info = client.poll_to_completion(
            probe_state.run_id,
            interval_seconds=ctx.poll_interval_seconds,
            timeout_seconds=ctx.poll_timeout_seconds,
            sleep=ctx.sleep,
        )
        events = client.get_event_log(probe_state.run_id)
    except DagsterRunNotFoundError as e:
        _set(state, probe_state, ctx, status=RoundTripStatus.FAILED,
             error=f"run not found: {e}")
        return
    except DagsterGraphQLError as e:
        # A timeout or transport error says nothing about the run: stay
        # LAUNCHED so the next invocation reconciles instead of duplicating.
        _set(state, probe_state, ctx, status=RoundTripStatus.LAUNCHED,
             error=f"poll/event-log failed, will reconcile: {e}")
        return

    from ..classes import Representative, Runnability

    pseudo_rep = Representative(
        repo=PROBES_LOCATION_NAME,
        git_sha=probe.class_hash[:12],
        asset_key=list(probe.probe_asset_key),
        runnability=Runnability.RUNNABLE,
        runnability_reason="round-trip probe",
    )
    record = build_run_record(
        qual_id=ctx.qual_id, side=ctx.side, class_hash=probe.class_hash,
        rep=pseudo_rep, run_status=info, events=events,
        partition_key=probe_state.partition_key,
    )
    ctx.registry.put_roundtrip_run_record(
        ctx.qual_id, ctx.side, probe.class_hash, info.run_id,
        record.model_dump_json().encode("utf-8"),
    )
    _set(
        state, probe_state, ctx,
        status=RoundTripStatus.PASSED if info.succeeded else RoundTripStatus.FAILED,
        run_id=info.run_id,
        error=None if info.succeeded else record.error,
    )


def _summarize(qual_id: str, side: str, state: RoundTripRunState) -> RoundTripSummary:
    counts: Dict[RoundTripStatus, int] = {s: 0 for s in RoundTripStatus}
    for p in state.probes.values():
        counts[p.status] += 1
    return RoundTripSummary(
        qual_id=qual_id, side=side, probe_total=len(state.probes),
        pending=counts[RoundTripStatus.PENDING],
        launched=counts[RoundTripStatus.LAUNCHED],
        passed=counts[RoundTripStatus.PASSED],
        failed=counts[RoundTripStatus.FAILED],
        skipped=counts[RoundTripStatus.SKIPPED],
    )

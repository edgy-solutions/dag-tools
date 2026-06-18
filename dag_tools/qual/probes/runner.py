"""``dagtools qual probes run --side <baseline|candidate>`` orchestrator.

End-to-end flow:

  1. Read the manifest + probe manifest from the registry.
     (Class matrix isn't needed — the probe manifest already carries
      the class_hash + module_name per probe.)
  2. Build (or load existing) :class:`ProbeRunState` for ``(qual_id, side)``.
  3. For each probe in the manifest:
       * PENDING — launch downstream asset, persist state.
       * LAUNCHED — desktop died; reconcile by polling the run_id.
       * FAILED — leave alone unless ``retry_failed=True``.
       * PASSED — skip (sacred).
  4. Per probe: launch (or reconcile) → poll-to-terminal → fetch event log →
     build :class:`RunRecord` (reusing the Q2 record schema) → persist
     under ``<side>/probes/runs/<class_hash>/<run_id>.json`` → update state.
  5. Write the per-side probes summary.

Recipe alignment: probes deploy to the ``dag-tools-probes`` user-code
location. We hardcode the location name here — that's the contract with
:mod:`dag_tools.probes_location`. The downstream asset's key is
``<module_name>_downstream`` (deps pulls the upstream automatically),
matching what the Q5 generator emits.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Callable, Dict, List, Optional

import yaml
from pydantic import BaseModel, ConfigDict

from ..graphql import (
    DagsterGraphQLClient,
    DagsterGraphQLError,
    resolve_auth_token,
)
from ..qualify import QualificationManifest
from ..registry import InventoryRegistry
from ..runs.launcher import build_run_record
from ..runs.records import RunRecord
from ..synthetic import ProbeManifest, ProbeModule
from .state import (
    ProbeRepState,
    ProbeRepStatus,
    ProbeRunState,
    default_local_probes_state_path,
    pending_or_resumable,
    transition,
    utcnow,
)


logger = logging.getLogger(__name__)


PROBES_LOCATION_NAME = "dag-tools-probes"
"""Contract with :mod:`dag_tools.probes_location` — the operator must
deploy the location under this name in their test deployment's
``workspace.yaml``."""

PROBES_REPOSITORY_NAME = "__repository__"
PROBES_JOB_NAME = "__ASSET_JOB"


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_probes_side(
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
) -> "ProbeSideOutcome":
    """Run probes for one side of the qualification.

    ``client_factory`` is injectable so tests can mock the GraphQL
    client without touching httpx; production callers leave it None.
    """
    manifest = _read_manifest(registry, qual_id)
    probe_manifest = _read_probe_manifest(registry, qual_id)
    local_state_path = local_state_path or default_local_probes_state_path(qual_id, side)

    state = _load_or_init_state(
        registry, qual_id, side, probe_manifest, local_state_path,
    )

    factory = client_factory or _default_client_factory
    client = factory(manifest)
    try:
        _drive_state(
            client=client,
            state=state,
            probe_manifest=probe_manifest,
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
    return ProbeSideOutcome(state=state, summary=summary)


# ---------------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------------


class ProbeSideSummary(BaseModel):
    """Aggregate of a probe side run — what Q6 reads to count synthetic
    coverage."""
    model_config = ConfigDict(extra="ignore")

    qual_id: str
    side: str
    probe_total: int
    pending: int
    launched: int
    passed: int
    failed: int
    skipped: int


class ProbeSideOutcome(BaseModel):
    """The end-state of one ``dagtools qual probes run --side`` invocation."""
    model_config = ConfigDict(extra="ignore")

    state: ProbeRunState
    summary: ProbeSideSummary


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


def _read_probe_manifest(registry: InventoryRegistry, qual_id: str) -> ProbeManifest:
    body = registry.read_probe_manifest(qual_id)
    if body is None:
        raise FileNotFoundError(
            f"no probe manifest for qual_id={qual_id!r}; "
            f"run `dagtools qual synthetic --id {qual_id}` first"
        )
    return ProbeManifest.model_validate_json(body)


def _load_or_init_state(
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    probe_manifest: ProbeManifest,
    local_state_path: Path,
) -> ProbeRunState:
    """Prefer the registry-mirrored state (so a different desktop can
    resume), then local file, then synthesize from the probe manifest."""
    registry_body = registry.read_probes_state(qual_id, side)
    if registry_body:
        state = ProbeRunState.model_validate_json(registry_body)
        _backfill_from_manifest(state, probe_manifest)
        return state

    if local_state_path.exists():
        state = ProbeRunState.model_validate_json(local_state_path.read_bytes())
        _backfill_from_manifest(state, probe_manifest)
        return state

    return _new_state(qual_id, side, probe_manifest)


def _new_state(
    qual_id: str, side: str, probe_manifest: ProbeManifest,
) -> ProbeRunState:
    now = utcnow()
    probes: Dict[str, ProbeRepState] = {}
    for probe in probe_manifest.probes:
        probes[probe.class_hash] = ProbeRepState(
            class_hash=probe.class_hash,
            module_name=probe.module_name,
            status=ProbeRepStatus.PENDING,
            last_updated=now,
        )
    return ProbeRunState(
        qual_id=qual_id,
        side=side,
        started_at=now,
        updated_at=now,
        probes=probes,
    )


def _backfill_from_manifest(
    state: ProbeRunState, probe_manifest: ProbeManifest,
) -> None:
    """Add any newly-introduced probes (e.g. after a re-run of qual
    synthetic --allow-overwrite that added a class) as PENDING."""
    now = utcnow()
    for probe in probe_manifest.probes:
        if probe.class_hash in state.probes:
            continue
        state.probes[probe.class_hash] = ProbeRepState(
            class_hash=probe.class_hash,
            module_name=probe.module_name,
            status=ProbeRepStatus.PENDING,
            last_updated=now,
        )


def _save_state(
    state: ProbeRunState,
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
        registry.put_probes_state(qual_id, side, body)
    except Exception as e:
        logger.warning("_save_state: registry mirror failed (%s); local copy is good", e)


def _drive_state(
    *,
    client: DagsterGraphQLClient,
    state: ProbeRunState,
    probe_manifest: ProbeManifest,
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
    probe_lookup = {p.class_hash: p for p in probe_manifest.probes}

    for probe_state in pending_or_resumable(state):
        if only_class and probe_state.class_hash != only_class:
            continue
        if probe_state.status == ProbeRepStatus.FAILED and not retry_failed:
            continue

        probe = probe_lookup.get(probe_state.class_hash)
        if probe is None:
            # Defensive: probe manifest lost track. Mark and move on.
            probe_state = transition(
                probe_state, status=ProbeRepStatus.SKIPPED,
                error="probe not present in current probe manifest",
            )
            state.probes[probe_state.class_hash] = probe_state
            _save_state(state, registry, qual_id, side, local_state_path)
            continue

        # --- LAUNCHED reconciliation: don't relaunch, poll the existing run_id.
        if probe_state.status == ProbeRepStatus.LAUNCHED and probe_state.run_id:
            _reconcile_or_finalize(
                client=client, state=state, probe_state=probe_state, probe=probe,
                registry=registry, qual_id=qual_id, side=side,
                local_state_path=local_state_path,
                poll_interval_seconds=poll_interval_seconds,
                poll_timeout_seconds=poll_timeout_seconds,
                sleep=sleep,
            )
            continue

        # --- PENDING or FAILED-with-retry: launch fresh.
        try:
            run_id = _launch_probe(client, probe, qual_id=qual_id, side=side,
                                   manifest=manifest)
        except DagsterGraphQLError as e:
            probe_state = transition(
                probe_state, status=ProbeRepStatus.FAILED,
                error=f"launch failed: {e}", bump_attempts=True,
            )
            state.probes[probe_state.class_hash] = probe_state
            _save_state(state, registry, qual_id, side, local_state_path)
            continue

        probe_state = transition(
            probe_state, status=ProbeRepStatus.LAUNCHED,
            run_id=run_id, bump_attempts=True,
        )
        state.probes[probe_state.class_hash] = probe_state
        _save_state(state, registry, qual_id, side, local_state_path)

        _reconcile_or_finalize(
            client=client, state=state, probe_state=probe_state, probe=probe,
            registry=registry, qual_id=qual_id, side=side,
            local_state_path=local_state_path,
            poll_interval_seconds=poll_interval_seconds,
            poll_timeout_seconds=poll_timeout_seconds,
            sleep=sleep,
        )


def _launch_probe(
    client: DagsterGraphQLClient,
    probe: ProbeModule,
    *,
    qual_id: str,
    side: str,
    manifest: QualificationManifest,
) -> str:
    """Submit the launch mutation for one probe's downstream asset.

    We launch only the downstream — its deps pulls the upstream
    automatically, which is enough to exercise the IO manager round-
    trip the probe's identity assertion checks. Tags mirror the Q2
    runs convention so probe runs are filterable from regular traffic.
    """
    downstream_asset_key = [f"{probe.module_name}_downstream"]
    tags = {
        "dagtools/qual": qual_id,
        "dagtools/side": side,
        "dagtools/probe": "true",
        "dagtools/class_hash": probe.class_hash,
    }
    run_config: Dict = {}
    if manifest.staging_overrides:
        run_config["__staging_overrides__"] = manifest.staging_overrides
    return client.launch_asset_run(
        location_name=PROBES_LOCATION_NAME,
        repository_name=PROBES_REPOSITORY_NAME,
        job_name=PROBES_JOB_NAME,
        asset_selection=[downstream_asset_key],
        run_config=run_config,
        tags=tags,
    )


def _reconcile_or_finalize(
    *,
    client: DagsterGraphQLClient,
    state: ProbeRunState,
    probe_state: ProbeRepState,
    probe: ProbeModule,
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    local_state_path: Path,
    poll_interval_seconds: float,
    poll_timeout_seconds: float,
    sleep: Optional[Callable[[float], None]],
) -> None:
    try:
        info = client.poll_to_completion(
            probe_state.run_id,
            interval_seconds=poll_interval_seconds,
            timeout_seconds=poll_timeout_seconds,
            sleep=sleep,
        )
        events = client.get_event_log(probe_state.run_id)
    except DagsterGraphQLError as e:
        probe_state = transition(
            probe_state, status=ProbeRepStatus.FAILED,
            error=f"poll/event-log failed: {e}",
        )
        state.probes[probe_state.class_hash] = probe_state
        _save_state(state, registry, qual_id, side, local_state_path)
        return

    record = _build_probe_record(
        qual_id=qual_id, side=side, probe=probe,
        run_status=info, events=events,
    )
    registry.put_probe_run_record(
        qual_id=qual_id, side=side,
        class_hash=probe.class_hash, run_id=info.run_id,
        body=record.model_dump_json().encode("utf-8"),
    )
    new_status = ProbeRepStatus.PASSED if info.succeeded else ProbeRepStatus.FAILED
    probe_state = transition(
        probe_state, status=new_status,
        run_id=info.run_id,
        error=record.error if not info.succeeded else None,
    )
    state.probes[probe_state.class_hash] = probe_state
    _save_state(state, registry, qual_id, side, local_state_path)


def _build_probe_record(
    *,
    qual_id: str,
    side: str,
    probe: ProbeModule,
    run_status,
    events,
) -> RunRecord:
    """Build a Q2 ``RunRecord`` for the probe run.

    We reuse the Q2 record schema verbatim — Q6 can diff probe runs the
    same way it diffs runnable-rep runs. The asset_key persisted is the
    downstream's, which is what was actually selected for materialization.
    Build via a fake ``Representative`` so we can reuse the existing
    ``build_run_record`` flatten logic for materialization / asset-check
    / failure parsing.
    """
    from ..classes import Representative, Runnability

    pseudo_rep = Representative(
        repo="dag-tools-probes",
        git_sha=probe.class_hash[:12],
        asset_key=[f"{probe.module_name}_downstream"],
        runnability=Runnability.SYNTHETIC_REQUIRED,
        runnability_reason="probe",
    )
    return build_run_record(
        qual_id=qual_id, side=side, class_hash=probe.class_hash,
        rep=pseudo_rep, run_status=run_status, events=events,
    )


def _write_summary(
    registry: InventoryRegistry,
    qual_id: str,
    side: str,
    state: ProbeRunState,
) -> ProbeSideSummary:
    counts: Dict[ProbeRepStatus, int] = {s: 0 for s in ProbeRepStatus}
    for p in state.probes.values():
        counts[p.status] = counts.get(p.status, 0) + 1
    summary = ProbeSideSummary(
        qual_id=qual_id,
        side=side,
        probe_total=len(state.probes),
        pending=counts[ProbeRepStatus.PENDING],
        launched=counts[ProbeRepStatus.LAUNCHED],
        passed=counts[ProbeRepStatus.PASSED],
        failed=counts[ProbeRepStatus.FAILED],
        skipped=counts[ProbeRepStatus.SKIPPED],
    )
    body = summary.model_dump_json().encode("utf-8")
    try:
        registry.put_probes_summary(qual_id, side, body, allow_overwrite=True)
    except Exception as e:
        logger.warning("_write_summary: registry write failed: %s", e)
    return summary


def _default_client_factory(manifest: QualificationManifest) -> DagsterGraphQLClient:
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

"""Launch one representative and assemble its :class:`RunRecord`.

Two responsibilities:

  * ``launch_representative`` — assemble the GraphQL launch params from a
    representative + the manifest's deployment config, submit the launch
    mutation, return the new ``run_id``. Pure shim around the GraphQL client.

  * ``build_run_record`` — given a terminal status + the run's event log,
    flatten into a :class:`RunRecord` with materializations, asset-check
    results, and the union of metadata keys. Soft-failing per event so
    one malformed entry doesn't drop the whole record.

The runner module composes these two for the resumable orchestrator.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..classes import Representative
from ..graphql import DagsterGraphQLClient, EventLogEntry, RunStatusInfo
from ..qualify import QualificationManifest
from .records import (
    AssetCheckResultSummary,
    MaterializationEventSummary,
    RunRecord,
)


logger = logging.getLogger(__name__)


DEFAULT_REPOSITORY_NAME = "__repository__"
"""Single-repo locations expose ``__repository__`` — the safe default
when the inventory record doesn't tell us which repo the asset is in
(it doesn't, today; we infer location from the inventory's ``location``
field but ``repository_name`` isn't captured)."""

DEFAULT_JOB_NAME = "__ASSET_JOB"
"""Modern Dagster wraps assets in an implicit job called ``__ASSET_JOB``.
When the inventory's ``job_names`` is empty we fall back to this."""


# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------


def launch_representative(
    client: DagsterGraphQLClient,
    rep: Representative,
    *,
    qual_id: str,
    side: str,
    manifest: QualificationManifest,
    location_name: Optional[str] = None,
    job_name: Optional[str] = None,
    run_config: Optional[Dict[str, Any]] = None,
) -> str:
    """Submit the launch mutation for one representative.

    Args:
      client: an already-authenticated GraphQL client.
      rep: the representative to materialize.
      qual_id, side: persisted as the ``dagtools/qual`` and
        ``dagtools/side`` tags so qualification runs are filterable from
        normal traffic on the test deployment.
      manifest: read to honor the ``staging_overrides`` pointer and any
        per-deployment defaults.
      location_name: overrides per-rep location; falls back to whatever
        the survey recorded on the AssetRecord.
      job_name: overrides per-rep job; falls back to
        :data:`DEFAULT_JOB_NAME`.
      run_config: explicit run config; merged on top of the staging
        overrides resolved from the manifest.

    Returns the new ``run_id``.
    """
    tags = {
        "dagtools/qual": qual_id,
        "dagtools/side": side,
        "dagtools/class_hash": _class_hash_from_rep(rep),
        "dagtools/asset_key": "/".join(rep.asset_key),
    }

    resolved_config = dict(run_config or {})
    # Staging overrides are deliberately layered first so per-rep config
    # can shadow them. (Right now nothing layers anything on top — this
    # exists so callers can supply asset-specific config without re-reading
    # the manifest.)
    if manifest.staging_overrides:
        resolved_config.setdefault("__staging_overrides__", manifest.staging_overrides)

    return client.launch_asset_run(
        location_name=location_name or "default",
        repository_name=DEFAULT_REPOSITORY_NAME,
        job_name=job_name or DEFAULT_JOB_NAME,
        asset_selection=[list(rep.asset_key)],
        run_config=resolved_config,
        tags=tags,
    )


def _class_hash_from_rep(rep: Representative) -> str:
    """Representative doesn't carry its class_hash directly. The runner
    stitches it from the matrix. Until then return empty so the tag
    survives without breaking."""
    return ""


# ---------------------------------------------------------------------------
# RunRecord assembly
# ---------------------------------------------------------------------------


def build_run_record(
    *,
    qual_id: str,
    side: str,
    class_hash: str,
    rep: Representative,
    run_status: RunStatusInfo,
    events: List[EventLogEntry],
) -> RunRecord:
    """Flatten a terminal RunStatusInfo + event log into a RunRecord.

    Soft-failing per event so one malformed entry doesn't drop the whole
    record.
    """
    materializations: List[MaterializationEventSummary] = []
    check_results: List[AssetCheckResultSummary] = []
    metadata_keys: List[str] = []
    failure_steps: List[str] = []

    for ev in events:
        # event_type is itself a property that can raise on malformed inputs;
        # capture it inside the try and use a safe fallback in the log call.
        event_type_for_log = "<unknown>"
        try:
            event_type = ev.event_type
            event_type_for_log = event_type
            if event_type in (
                "MaterializationEvent",
                "AssetMaterializationPlannedEvent",
            ) and ev.asset_key:
                materializations.append(MaterializationEventSummary(
                    asset_key=list(ev.asset_key),
                    step_key=ev.step_key,
                    timestamp=ev.timestamp,
                    metadata_keys=list(ev.metadata_keys or []),
                ))
                for k in ev.metadata_keys or []:
                    if k and k not in metadata_keys:
                        metadata_keys.append(k)

            elif event_type == "AssetCheckResultEvent":
                raw = ev.raw or {}
                ak = (raw.get("assetKey") or {}).get("path") or []
                check_results.append(AssetCheckResultSummary(
                    asset_key=list(ak),
                    check_name=raw.get("checkName") or raw.get("name"),
                    passed=raw.get("success"),
                    severity=raw.get("severity"),
                ))

            elif event_type == "ExecutionStepFailureEvent":
                if ev.step_key:
                    failure_steps.append(ev.step_key)
        except Exception as e:
            logger.warning(
                "build_run_record: skipping bad event %r: %s",
                event_type_for_log, e,
            )

    started_at = _epoch_to_dt(run_status.start_time)
    ended_at = _epoch_to_dt(run_status.end_time)
    duration_seconds = None
    if started_at is not None and ended_at is not None:
        duration_seconds = (ended_at - started_at).total_seconds()

    error = None
    if not run_status.succeeded:
        # Best-effort: the last failure event's message is usually the most actionable.
        for ev in reversed(events):
            if ev.event_type == "ExecutionStepFailureEvent" and ev.message:
                error = ev.message
                break

    return RunRecord(
        qual_id=qual_id,
        side=side,
        class_hash=class_hash,
        asset_key=list(rep.asset_key),
        repo=rep.repo,
        git_sha=rep.git_sha,
        run_id=run_status.run_id,
        success=run_status.succeeded,
        status=run_status.status,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=duration_seconds,
        materialization_events=materializations,
        asset_check_results=check_results,
        metadata_keys=metadata_keys,
        error=error,
        failure_step_keys=failure_steps,
        event_count=len(events),
    )


def _epoch_to_dt(ts: Optional[float]) -> Optional[datetime]:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None

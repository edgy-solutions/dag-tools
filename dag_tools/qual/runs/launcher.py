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
from ..graphql import (
    DagsterGraphQLClient,
    DagsterSubsetNotSupportedError,
    EventLogEntry,
    RunStatusInfo,
)
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
    launch_info: Optional[Dict[str, Dict[str, Any]]] = None,
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
      launch_info: per-asset facts from
        :meth:`DagsterGraphQLClient.get_asset_launch_info`, used to expand
        multi_asset siblings into the selection and to pick a partition.
        Optional — omitting it reproduces the old single-key behaviour.

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

    info = launch_info or {}
    selection, partition_key = plan_asset_selection(list(rep.asset_key), info)
    if partition_key is not None:
        # Dagster resolves context.partition_key from the RUN TAG, so this
        # is what turns an asset job into a partitioned run.
        tags[PARTITION_NAME_TAG] = partition_key

    def _launch(sel: List[List[str]]) -> str:
        return client.launch_asset_run(
            location_name=location_name or "default",
            repository_name=DEFAULT_REPOSITORY_NAME,
            job_name=job_name or DEFAULT_JOB_NAME,
            asset_selection=sel,
            run_config=resolved_config,
            tags=tags,
        )

    try:
        return _launch(selection)
    except DagsterSubsetNotSupportedError:
        # The representative belongs to a multi_asset that refuses a
        # partial selection. Widen to the op's siblings and try once more.
        # Deliberately NOT done up front: a subsettable definition
        # (@dbt_assets above all) accepts the one-key selection, and
        # widening it there materializes the whole project per
        # representative. See plan_asset_selection.
        widened = expand_to_op_siblings(list(rep.asset_key), info)
        if len(widened) <= len(selection):
            # Nothing to widen to -- the deployment reported no siblings,
            # so the refusal is about something else and re-raising keeps
            # the original, accurate message.
            raise
        logger.info(
            "qual: %s refused a partial selection; retrying with its %d "
            "op-mates (non-subsettable multi_asset)",
            "/".join(rep.asset_key),
            len(widened) - 1,
        )
        return _launch(widened)


def is_launchable(
    asset_key: List[str],
    launch_info: Dict[str, Dict[str, Any]],
) -> tuple[bool, str]:
    """Whether the DEPLOYMENT will accept a launch for this asset key.

    Returns ``(ok, reason)``; ``reason`` is empty when ok.

    Only one thing is checked, and only when the deployment answered
    positively: ``isExecutable == False``. External/source assets — a bare
    ``AssetSpec``, or the stub Dagster auto-creates for a
    ``deps=[AssetKey(...)]`` naming a key defined nowhere in the
    Definitions — have no compute, and selecting one is rejected before
    the run starts::

        DagsterInvalidDefinitionError: Selected keys must be a subset of
        existing executable asset keys.

    A dlt or Sling ingest declares its upstream source tables exactly that
    way, so a fleet with one such pipeline gets one unlaunchable
    representative per source table.

    Deliberately silent when the key is absent from ``launch_info`` or the
    deployment didn't report ``isExecutable``: a missing answer means we
    don't know, and refusing to launch on "don't know" would turn one
    failed introspection query into a whole side skipped as green.
    """
    info = launch_info.get("/".join(asset_key))
    if info and info.get("is_executable") is False:
        return False, (
            "not executable on the deployment (external/source asset — "
            "Dagster has no compute for it). Assets a dlt/Sling-style "
            "pipeline names via `deps=[AssetKey(...)]` appear this way. "
            "Re-run `dagtools survey` to record is_executable in the "
            "inventory so Q1 classifies it OBSERVE_ONLY up front."
        )
    return True, ""


PARTITION_NAME_TAG = "dagster/partition"
"""Dagster reads the run's partition from this tag. Hardcoded rather than
imported from ``dagster`` because the qualification CLI is installed
WITHOUT dagster -- it drives a deployment over GraphQL, and its extras
deliberately pin no dagster version so it can run against the one being
qualified."""


def plan_asset_selection(
    asset_key: List[str],
    launch_info: Dict[str, Dict[str, Any]],
) -> tuple[List[List[str]], Optional[str]]:
    """Work out what to select, and which partition, for one asset.

    Returns ``(asset_selection, partition_key)``.

    The selection is ONE asset -- the representative and nothing else.
    That is the whole point of a representative: Q2 compares the behaviour
    of one asset per equivalence class, so materializing its op-mates
    costs compute the qualification never looks at.

    This used to pre-emptively select every asset sharing the
    representative's op, to get ahead of a non-subsettable
    ``multi_asset``::

        DagsterInvalidSubsetError: ... contains asset keys
        [order_lines, orders] ... but attempted to select only
        [order_lines]. This AssetsDefinition does not support subsetting.

    But dagster raises that ONLY for ``can_subset=False``. ``@dbt_assets``
    is subsettable and translates a one-key selection into a one-model
    ``--select``, so the widening was pure cost there -- a consumer with a
    22-model dbt project had every representative launch build all 22, and
    the op-mates that the selection dragged in brought their own upstream
    requirements, so most of them failed. ``AssetNode`` exposes no
    ``canSubset`` field (checked against the 1.13 schema), so the
    subsettable case cannot be told apart up front. It is told apart
    AFTERWARDS instead: :func:`launch_representative` retries with
    :func:`expand_to_op_siblings` when, and only when, the deployment
    answers ``InvalidSubsetError``.

    **Partitioned assets** are still resolved here. Launched with no
    partition, the run is not a partitioned run, and the asset dies as
    soon as it reads ``context.partition_key``. The LAST partition is
    chosen: for a time-based definition that is the most recent period,
    which is the one whose data is most likely to exist and the closest
    analogue to what production would just have run.

    Degrades to ``([asset_key], None)`` when the deployment told us
    nothing.
    """
    info = launch_info.get("/".join(asset_key))
    if not info:
        return [list(asset_key)], None

    partition_key: Optional[str] = None
    if info.get("is_partitioned"):
        keys = info.get("partition_keys") or []
        partition_key = str(keys[-1]) if keys else None

    return [list(asset_key)], partition_key


def expand_to_op_siblings(
    asset_key: List[str],
    launch_info: Dict[str, Dict[str, Any]],
) -> List[List[str]]:
    """Every executable asset produced by the same op as ``asset_key``.

    Only used to RETRY a launch the deployment rejected with
    ``InvalidSubsetError`` -- i.e. a ``multi_asset`` declared
    ``can_subset=False``, which must be selected whole or not at all.
    Assets produced by one op share an ``opNames`` entry, so the siblings
    are recoverable from what the deployment already told us.

    Returns a deterministically ordered selection that always contains
    ``asset_key`` itself, so the result is safe to launch even when the
    deployment reported no siblings.
    """
    key_str = "/".join(asset_key)
    info = launch_info.get(key_str)
    if not info:
        return [list(asset_key)]

    selection: List[List[str]] = [list(asset_key)]
    op_names = set(info.get("op_names") or [])
    if op_names:
        for other_key, other in launch_info.items():
            if other_key == key_str:
                continue
            # An external asset never shares a real op, but it can carry an
            # empty/synthetic opNames that set-intersects with nothing --
            # skip it explicitly so a sibling sweep can never drag one into
            # the selection and fail the launch for the whole op.
            if other.get("is_executable") is False:
                continue
            if op_names & set(other.get("op_names") or []):
                selection.append(list(other["asset_key"]))

    # Deterministic order so a re-run produces an identical selection.
    selection.sort()
    return selection


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

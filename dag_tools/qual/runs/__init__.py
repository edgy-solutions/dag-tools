"""Phase 2 Q2/Q4 — resumable per-representative run execution.

Public surface:
  * :func:`run_side` — the orchestrator that ``dagtools qual run`` wraps.
  * :class:`SideOutcome` / :class:`SideSummary` — what callers get back.
  * :class:`RepStatus` / :class:`RepState` / :class:`QualRunState` — the
    resumable state machine.
  * :class:`RunRecord` — the per-rep record persisted to the registry.
  * :func:`launch_representative` and :func:`build_run_record` — building
    blocks the runner composes; importable for the inevitable bespoke
    one-off probe Q5 will need.
  * :func:`resolve_staging_overrides` — turns the manifest's
    ``staging_overrides`` POINTER into a run config document. The launchers
    will not accept an unresolved pointer; see :mod:`.staging`.
"""
from .launcher import build_run_record, launch_representative
from .records import (
    AssetCheckResultSummary,
    MaterializationEventSummary,
    RunRecord,
)
from .runner import SideOutcome, SideSummary, run_side
from .staging import (
    RUN_CONFIG_TOP_LEVEL_KEYS,
    StagingOverridesError,
    StagingOverridesNotResolved,
    merge_run_config,
    resolve_staging_overrides,
    storage_fetch,
)
from .state import (
    QualRunState,
    RepState,
    RepStatus,
    default_local_state_path,
    pending_or_resumable,
    rep_id_for,
    transition,
)

__all__ = [
    # orchestrator
    "run_side",
    "SideOutcome",
    "SideSummary",
    # state
    "RepStatus",
    "RepState",
    "QualRunState",
    "pending_or_resumable",
    "transition",
    "rep_id_for",
    "default_local_state_path",
    # records
    "RunRecord",
    "MaterializationEventSummary",
    "AssetCheckResultSummary",
    # building blocks
    "launch_representative",
    "build_run_record",
    # staging overrides
    "resolve_staging_overrides",
    "merge_run_config",
    "storage_fetch",
    "StagingOverridesError",
    "StagingOverridesNotResolved",
    "RUN_CONFIG_TOP_LEVEL_KEYS",
]

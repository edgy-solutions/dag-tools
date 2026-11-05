"""``dagtools qual stage`` -- materialize the upstreams Q2 will load.

Public surface:
  * :func:`run_stage` -- the orchestrator the CLI wraps.
  * :func:`build_stage_plan` -- the pure selection/grouping step.
  * :class:`StageState` / :class:`StageGroupState` / :class:`StageStatus` /
    :class:`StageRecord` -- its own state slot, distinct from Q2's and the
    probes'.
"""
from .plan import (
    KeyDependents,
    LaunchInfoUnavailable,
    StageGroupPlan,
    StagePlan,
    build_stage_plan,
    reps_ready,
)
from .runner import (
    StageOutcome,
    StageTooLarge,
    build_stage_record,
    key_outcomes_from_events,
    launch_stage_group,
    run_stage,
)
from .state import (
    StageGroupState,
    StageRecord,
    StageState,
    StageStatus,
    default_local_stage_state_path,
)

__all__ = [
    "run_stage",
    "StageOutcome",
    "StageTooLarge",
    "LaunchInfoUnavailable",
    "build_stage_plan",
    "reps_ready",
    "StagePlan",
    "StageGroupPlan",
    "KeyDependents",
    "launch_stage_group",
    "build_stage_record",
    "key_outcomes_from_events",
    "StageState",
    "StageGroupState",
    "StageStatus",
    "StageRecord",
    "default_local_stage_state_path",
]

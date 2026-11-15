"""Per-pipeline ``schedule:`` for the dlt component (built in Python, never post_processing)."""
from typing import Any, Dict, List, Optional

from dagster import (
    AssetSelection,
    DagsterRunStatus,
    DefaultScheduleStatus,
    RunRequest,
    RunsFilter,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SkipReason,
    define_asset_job,
)

from .schema import DltPipelineScheduleSchema

#: A run in any of these states means the previous tick is still working.
IN_PROGRESS_STATUSES = [
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
]


def build_pipeline_schedule(
    pipeline_name: str,
    asset_keys: List[Any],
    schedule: Dict[str, Any],
    job_name: Optional[str] = None,
) -> Dict[str, List[Any]]:
    """Build ``<pipeline>_job`` and ``<pipeline>_schedule`` for one pipeline.

    With ``job_name`` the schedule targets that existing job (another contribution
    defines it) and builds none of its own; the skip guard filters by that name.

    The job selects exactly ``asset_keys`` (never ``all()``, which would not
    include external source specs and would pass vacuously). The schedule is
    STOPPED unless ``enabled: true`` and skips while a run of its job is in
    progress. A malformed block or an empty pipeline raises, failing the load.
    """
    cfg = DltPipelineScheduleSchema.model_validate(schedule)
    if not asset_keys:
        raise ValueError(
            f"pipeline {pipeline_name!r} declares a schedule but produced no assets; "
            "a job over nothing would pass vacuously."
        )

    own_job = job_name is None
    if own_job:
        job_name = f"{pipeline_name}_job"
        job = define_asset_job(job_name, selection=AssetSelection.assets(*asset_keys))

    def _evaluate(context: ScheduleEvaluationContext):
        records = context.instance.get_run_records(
            RunsFilter(job_name=job_name, statuses=IN_PROGRESS_STATUSES), limit=1
        )
        if records:
            return SkipReason(f"a run of {job_name} is still in progress")
        return RunRequest()

    kwargs: Dict[str, Any] = {}
    if cfg.timezone is not None:
        kwargs["execution_timezone"] = cfg.timezone
    sched = ScheduleDefinition(
        name=f"{pipeline_name}_schedule",
        cron_schedule=cfg.cron,
        **({"job": job} if own_job else {"job_name": job_name}),
        execution_fn=_evaluate,
        default_status=(
            DefaultScheduleStatus.RUNNING if cfg.enabled else DefaultScheduleStatus.STOPPED
        ),
        **kwargs,
    )
    return {"jobs": [job] if own_job else [], "schedules": [sched]}

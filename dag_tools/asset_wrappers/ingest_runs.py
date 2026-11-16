"""Run recording for the dlt pipeline component (``ingest_runs: true``).

One row per run, failed or not, in the pipeline's own dataset, written by a
SECOND dlt run after the data run; the original failure is then re-raised so a
failed ingest stays a failed Dagster run. Same shape and the same invariant as
the SystemLink kind's own table (which is why this is wired into the BASE
``SourceKind.execute`` and never around the call site: that kind overrides
``execute`` and already writes its own).

The row carries the ROOT error's class name and a digest of the message, never
the message: an exception string routinely carries a DSN, a row of source data
or a token, and this table is as readable as any other in the warehouse.

If the row cannot be written, which error propagates depends on whether there
is a competing one, and the asymmetry is the point. After a FAILED data run the
write error is logged and the ORIGINAL failure propagates: an instrument may
never break the thing it measures, and a run reported as having died of its
audit row sends the operator after the ingest instead of the warehouse. After a
SUCCESSFUL one there is no competing cause to misattribute, so the write error
propagates and the run goes red -- the alternative is a green run with no row,
which breaks "a run is always recorded" silently, and an audit nobody can see a
hole in is worse than one that fails loudly.
"""
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional

import dlt

logger = logging.getLogger(__name__)

TABLE = "ingest_runs"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def root_error(failure: BaseException) -> BaseException:
    """dlt wraps what a resource raised; find the innermost error in the cause chain."""
    seen, exc, last = set(), failure, failure
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        last = exc
        exc = exc.__cause__ or exc.__context__
    return last


def root_error_name(failure: BaseException) -> str:
    """The class name of the innermost error."""
    return type(root_error(failure)).__name__


def message_digest(exc: BaseException) -> str:
    """Digest ONE exception's message, for a caller that has already chosen the exception.

    ``error_digest`` chooses the innermost error; a caller whose ``error_type``
    column names a different error in the chain must digest *that* one, or the
    two columns describe two exceptions. This is the seam that lets it.
    """
    return hashlib.sha256(str(exc).encode()).hexdigest()[:12]


def error_digest(failure: BaseException) -> str:
    """A digest of the ROOT error's message, which is the only stable thing to digest.

    The digest exists so two rows recording the same failure carry the same
    value; that is the whole reason a digest stands in for a message nobody may
    store. Digesting the exception dlt hands over destroys exactly that, because
    its message embeds a per-run load id -- measured, two runs of one identical
    root failure digesting to `1278fb1d13d6` and `046594cc097a`. The root's own
    message is stable across runs, and it is also the error `error_type` already
    names, so the two columns describe one exception rather than two.
    """
    return message_digest(root_error(failure))


def _row_counts(pipeline: Any) -> Dict[str, int]:
    try:
        counts = pipeline.last_trace.last_normalize_info.row_counts
    except Exception:
        return {}
    return {t: int(n) for t, n in (counts or {}).items() if not t.startswith("_dlt_")}


def _mode(config: Any) -> str:
    """Derived from what the component knows: does any table declare an incremental cursor?"""
    hints = getattr(config, "hints", None) or {}
    incremental = any(isinstance(h, dict) and h.get("incremental") for h in hints.values())
    return "incremental" if incremental else "full"


def build_row(context: Any, config: Any, pipeline: Any, started: str,
              failure: Optional[BaseException]) -> Dict[str, Any]:
    counts = _row_counts(pipeline)
    return {
        "run_id": getattr(getattr(context, "run", None), "run_id", None) or "local",
        "source_system": config.name,
        "mode": _mode(config),
        "status": "FAILED" if failure else "SUCCESS",
        "error_type": root_error_name(failure) if failure else None,
        "error_digest": error_digest(failure) if failure else None,
        "started_at": started,
        "finished_at": _now(),
        "rows_total": sum(counts.values()),
        "row_counts": json.dumps(counts, sort_keys=True),
    }


def write_row(pipeline: Any, row: Dict[str, Any]) -> None:
    # The two error columns are null on a successful row, and dlt creates no
    # column from a null: declared, so the table has one shape whatever ran first.
    # (One dict per column: dlt writes the column name into the dict it is handed.)
    @dlt.resource(
        name=TABLE, write_disposition="append",
        columns={
            "error_type": {"data_type": "text", "nullable": True},
            "error_digest": {"data_type": "text", "nullable": True},
        },
    )
    def _runs() -> Iterator[Dict[str, Any]]:
        yield row

    pipeline.run(_runs())


def execute_recorded(context: Any, dlt_resource: Any, *, pipeline: Any, config: Any,
                     run_kwargs: Dict[str, Any]):
    """The default body of a generated asset, plus the run row."""
    started = _now()
    failure: Optional[BaseException] = None
    try:
        yield from dlt_resource.run(context=context, **run_kwargs)
    except Exception as exc:  # recorded, then re-raised after the row is written
        failure = exc
    # Built BEFORE the write, and held in a local so it stays that way: the row
    # counts come off `pipeline.last_trace`, which the recording run replaces.
    # As one expression this depends on argument evaluation order, and a later
    # edit that hoisted the write would silently record zeros.
    row = build_row(context, config, pipeline, started, failure)
    try:
        write_row(pipeline, row)
    except Exception:
        if failure is None:
            raise
        logger.exception("could not record the ingest run of a failed run")
    if failure is not None:
        raise failure

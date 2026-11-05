"""Declarative component: what the pipelines read, and which asset explains it.

Step 1 of the classification build (ADR-0064). One asset per component
instance; it fetches a window of query history, rolls it up through the pure
engine in `dag_tools.audit`, returns the report, and puts the headline numbers
on the materialization so the scope number is readable in the UI and
diffable between runs.

Shape, per `AGENTS.md` §2 (Declarative Component First): every knob is a YAML
field, the engine stays pure and Dagster-free, and the component is the only
thing that knows about Dagster. Per §5 the YAML is a Dagster-centric schema
(`schema.py`) rather than a pass-through of anyone's connection kwargs.

**Nothing here names a relation, a column or a level.** The history relation
and its five columns are required YAML values with no defaults; see
`dag_tools.audit.history` for why a default would be worse than an error.

**The read is executed by a resource, not by this component.** When
`history.resource_key` is set, the component builds the statement and hands it
to that resource's `fetch_rows`. The component therefore has no driver
dependency, no credential handling and no connection code, and a test drives
it with a stub. Step 2's tagged warehouse resource is an implementation of
that one method.

**The default topology is an export.** `history.rows_file` reads rows the
operator pulled themselves. The account that can read query history is often
not the one Dagster holds, and measurement should not wait on a grant.
"""
# NOTE: intentionally NO `from __future__ import annotations` -- the audit
# asset's `config: ReadAuditRunConfig` parameter must be a real class object
# at definition time or Dagster cannot resolve it as a pythonic Config type
# (it raises DagsterInvalidPythonicConfigDefinitionError naming the string).
# Same trap as `grist_ingest.component`.
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    Definitions,
    MetadataValue,
    asset,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from dag_tools.audit.history import (
    HistoryColumns,
    HistorySource,
    build_history_statement,
    rows_to_events,
)
from dag_tools.audit.reads import RunWindow, build_reads_report

from .schema import ReadAuditRunConfig, ReadAuditSpec


def _resolve_path(candidate: str, component_dir: Optional[str]) -> str:
    """Component-relative first, then as given -- same as the other components."""
    if component_dir:
        joined = os.path.join(component_dir, candidate)
        if os.path.exists(joined):
            return joined
    return candidate


def _load_json(path: str, what: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        parsed = json.load(handle)
    if isinstance(parsed, dict) and isinstance(parsed.get(what), list):
        return list(parsed[what])
    if not isinstance(parsed, list):
        raise ValueError(
            f"{path} should hold a JSON list of {what}, or an object with a "
            f"{what!r} key; got {type(parsed).__name__}"
        )
    return list(parsed)


def windows_from_run_rows(rows: Sequence[Dict[str, Any]]) -> List[RunWindow]:
    """Map Dagster run records (already reduced to plain rows) to windows.

    Epoch seconds in, aware UTC datetimes out. A run with no asset selection
    yields a window with no asset keys, which the engine treats as ambiguous
    rather than attributing its reads to nothing -- a job run that selected
    everything genuinely cannot say which asset issued a statement.

    Pure, and separate from the instance call for that reason: the mapping is
    the part worth pinning, and it needs no instance to pin.
    """
    windows: List[RunWindow] = []
    for row in rows:
        run_id = row.get("run_id")
        start = row.get("start_time")
        if not run_id or start is None:
            continue
        end = row.get("end_time")
        windows.append(
            RunWindow(
                run_id=str(run_id),
                asset_keys=[str(key) for key in (row.get("asset_keys") or [])],
                started_at=datetime.fromtimestamp(float(start), tz=timezone.utc),
                ended_at=(
                    None if end is None else datetime.fromtimestamp(float(end), tz=timezone.utc)
                ),
            )
        )
    return windows


def _instance_run_rows(context: AssetExecutionContext, limit: int) -> List[Dict[str, Any]]:
    """Reduce the instance's run records to the rows `windows_from_run_rows` takes.

    Soft: an instance that cannot answer leaves the audit with fewer windows
    and therefore a larger honest gap, which is strictly better than failing
    the report the board is waiting for.
    """
    try:
        records = context.instance.get_run_records(limit=limit)
    except Exception as exc:  # noqa: BLE001 -- fewer windows, not a failed audit
        context.log.warning("could not read run records from the instance: %s", exc)
        return []

    rows: List[Dict[str, Any]] = []
    for record in records:
        run = getattr(record, "dagster_run", None)
        if run is None:
            continue
        selection = getattr(run, "asset_selection", None) or []
        rows.append(
            {
                "run_id": getattr(run, "run_id", None),
                "asset_keys": [key.to_user_string() for key in selection],
                "start_time": getattr(record, "start_time", None),
                "end_time": getattr(record, "end_time", None),
            }
        )
    return rows


def _fetch_rows_via_resource(
    context: AssetExecutionContext,
    spec: ReadAuditSpec,
    lookback_hours: int,
) -> List[Dict[str, Any]]:
    history = spec.history
    source = HistorySource(
        relation=history.relation or "",
        columns=HistoryColumns.model_validate(history.columns or {}),
        principal_filter=history.principal_filter,
    )
    until = datetime.now(timezone.utc)
    since = until - timedelta(hours=lookback_hours)
    statement, params = build_history_statement(source, since=since, until=until)

    resource = getattr(context.resources, history.resource_key)
    fetch = getattr(resource, "fetch_rows", None)
    if fetch is None:
        raise ValueError(
            f"resource '{history.resource_key}' does not expose fetch_rows("
            "statement, parameters); the read audit executes nothing itself"
        )
    context.log.info("reading history from %s hours back", lookback_hours)
    return list(fetch(statement, params))


def build_read_audit_asset(
    spec: ReadAuditSpec, component_dir: Optional[str]
) -> AssetsDefinition:
    """Factory, not a closure defined inline.

    `AGENTS.md` §2: Dagster introspects every parameter of an `@asset`
    function as an asset input, so the configuration has to arrive through the
    enclosing function rather than as arguments.
    """
    required = (
        {spec.history.resource_key}
        if spec.history.resource_key
        else set()
    )

    @asset(
        name=spec.asset_name,
        group_name=spec.group_name,
        io_manager_key=spec.io_manager_key,
        owners=spec.owners or None,
        tags=spec.tags or None,
        required_resource_keys=required,
        description=(
            "Which objects each asset reads, which assets read each object, "
            "and the objects no asset explains. Read-only."
        ),
    )
    def _read_audit(
        context: AssetExecutionContext, config: ReadAuditRunConfig
    ) -> Dict[str, Any]:
        lookback = config.lookback_hours or spec.history.lookback_hours

        if spec.history.rows_file:
            path = _resolve_path(spec.history.rows_file, component_dir)
            rows = _load_json(path, "events")
            context.log.info("read %d history rows from %s", len(rows), path)
        else:
            rows = _fetch_rows_via_resource(context, spec, lookback)
            context.log.info("read %d history rows through the resource", len(rows))

        events, failures = rows_to_events(rows)

        windows: List[RunWindow] = []
        if spec.runs.runs_file:
            runs_path = _resolve_path(spec.runs.runs_file, component_dir)
            for index, item in enumerate(_load_json(runs_path, "runs")):
                try:
                    windows.append(RunWindow.model_validate(item))
                except Exception as exc:  # noqa: BLE001 -- one bad window
                    failures.append(f"run {index}: {type(exc).__name__}: {exc}")
        if spec.runs.from_instance:
            windows.extend(
                windows_from_run_rows(_instance_run_rows(context, spec.runs.limit))
            )

        report = build_reads_report(events, windows)
        payload = json.loads(report.model_dump_json())
        payload["rows_read"] = len(rows)
        payload["row_failures"] = failures

        context.add_output_metadata(_materialization_metadata(payload, report, config))
        return payload

    return _read_audit


def _materialization_metadata(
    payload: Dict[str, Any], report: Any, config: ReadAuditRunConfig
) -> Dict[str, Any]:
    """The headline numbers, on the materialization.

    The gap is published decomposed as well as whole: a reader who sees only
    one number cannot tell work that a query tag fixes (step 2) from work that
    needs a `passthrough` decision (step 5), and those go to different people.
    """
    cap = max(0, config.max_listed)

    def listed(values: Sequence[str]) -> Any:
        if cap == 0:
            return MetadataValue.int(len(values))
        return MetadataValue.json(
            {"count": len(values), "shown": list(values[:cap])}
        )

    coverage = report.coverage
    metadata: Dict[str, Any] = {
        "rows_read": MetadataValue.int(payload["rows_read"]),
        "events": MetadataValue.int(report.event_count),
        "objects_read": MetadataValue.int(report.relation_count),
        "assets_attributed": MetadataValue.int(len(report.relations_per_asset)),
        "explained": MetadataValue.int(len(report.explained_relations)),
        "no_asset_explains": MetadataValue.int(len(report.unexplained_relations)),
        "gap_ambiguous": MetadataValue.int(len(report.ambiguous_relations)),
        "gap_untagged": MetadataValue.int(len(report.untagged_relations)),
        "gap_not_a_pipeline": MetadataValue.int(len(report.non_pipeline_relations)),
        "rows_not_read": MetadataValue.int(len(payload["row_failures"])),
        "unexplained": listed(report.unexplained_relations),
        "method_counts": MetadataValue.json(report.method_counts),
    }
    # None when nothing was read; 0/0 must not render as full coverage.
    if coverage is not None:
        metadata["coverage"] = MetadataValue.float(round(coverage, 4))
    return metadata


class ReadAuditComponent(Component, Resolvable, Model):
    """Audit which assets read which objects, from query history.

    Read-only: it issues one SELECT against a history relation (or reads an
    export) and writes a report. It changes nothing, grants nothing and needs
    no classification registry -- it is what the registry gets sized from.
    """

    history: Dict[str, Any]
    """Where the rows come from. Exactly one of `rows_file` or `resource_key`;
    with a resource, `relation` and all five `columns` are required."""

    runs: Dict[str, Any] = {}
    """Optional run windows, for reads no tag explains: `runs_file` and/or
    `from_instance`."""

    asset_name: str = "read_audit"
    """Name of the asset this component builds."""

    group_name: Optional[str] = None
    io_manager_key: Optional[str] = None
    owners: List[str] = []
    tags: Dict[str, str] = {}

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        spec = ReadAuditSpec.from_attributes(
            asset_name=self.asset_name,
            group_name=self.group_name,
            io_manager_key=self.io_manager_key,
            owners=self.owners,
            tags=self.tags,
            history=self.history,
            runs=self.runs,
        )
        component_dir = str(context.path) if getattr(context, "path", None) else None
        return Definitions(assets=[build_read_audit_asset(spec, component_dir)])

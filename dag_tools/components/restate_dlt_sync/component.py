import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import sqlalchemy as sa
import yaml
from dagster import (
    AssetKey,
    DagsterRunStatus,
    Definitions,
    RunRequest,
    RunsFilter,
    SkipReason,
    asset,
    define_asset_job,
    sensor,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from dagster_dlt import DagsterDltResource
from dag_tools.asset_wrappers.dlt_assets_parsing import create_dlt_assets
from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig

from .config import ControlTableSpec, MeiTableSpec, TableSpec


def _executable_asset_keys(items: List[Any]) -> List[AssetKey]:
    """Pull the executable AssetKeys out of what create_dlt_assets returned.

    create_dlt_assets returns a mix of AssetsDefinition (the real dlt
    extraction assets) and AssetSpec (external source-table placeholders).
    Only the AssetsDefinition keys are materializable, so only those belong
    in the dispatch asset's deps and the cycle job's selection.
    """
    keys: List[AssetKey] = []
    for item in items:
        if hasattr(item, "keys"):
            keys.extend(item.keys)
    return keys


def dlt_key_by_source_table(
    dlt_assets_group: List[Any], sources: List[str]
) -> Dict[str, AssetKey]:
    """Map each source table to the ONE dlt asset key that carries it.

    ``create_dlt_assets`` returns a single ``@multi_asset`` covering every
    table in the pipeline, so its ``.keys`` is the whole set. Handing that
    set to each table's ack dispatch made every dispatch depend on every
    table: with a dozen tables the graph is a complete bipartite mess, and
    the lineage claims PDM_ROUTING's acknowledgment is derived from
    PDM_BOM's data.

    The translator gives each output spec exactly one dep -- the external
    stub for the table it came from -- so the tail of that dep is the
    source table name. Matched case-insensitively because the dlt side
    lowercases table names while the config names them as Oracle does.

    Tables with no unique match are simply absent; the caller keeps the
    old all-keys behaviour for those, since a dispatch with NO upstream
    would run before the extraction rather than after it.
    """
    by_tail: Dict[str, List[AssetKey]] = {}
    for item in dlt_assets_group:
        if not hasattr(item, "keys"):
            continue  # external AssetSpec placeholder, not the multi_asset
        for spec in getattr(item, "specs", []) or []:
            for dep in spec.deps:
                path = dep.asset_key.path
                if path:
                    by_tail.setdefault(path[-1].lower(), []).append(spec.key)

    resolved: Dict[str, AssetKey] = {}
    for table in sources:
        hits = by_tail.get(str(table).lower(), [])
        if len(hits) == 1:
            resolved[table] = hits[0]
    return resolved


def load_mei_list(source_file: Optional[str], inline: List[str]) -> List[str]:
    """Read the MEI list from the overlay file, falling back to inline.

    The overlay is a git repo mounted into the pod, so the format is
    whatever was convenient to keep under version control. All three
    common shapes are accepted rather than forcing the list to be
    rewritten to suit us:

      * a YAML list, or a YAML mapping with a single list value
      * a JSON list
      * one MEI per line, with ``#`` comments and blanks ignored

    Read at call time, never cached: the whole point of an overlay is
    that changing it changes behaviour without a redeploy.
    """
    if not source_file:
        return [str(m).strip() for m in inline if str(m).strip()]

    path = Path(source_file)
    if not path.exists():
        raise FileNotFoundError(
            f"MEI overlay file not found: {source_file}. This is the list of "
            f"top-level Major End Items to request; without it PDM has "
            f"nothing to explode and the MEI-scoped tables stay empty."
        )

    raw = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()

    if suffix not in (".yaml", ".yml", ".json"):
        return [
            line.strip() for line in raw.splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]

    parsed: Any = yaml.safe_load(raw) if suffix != ".json" else json.loads(raw)

    if isinstance(parsed, dict):
        # A mapping wrapping the list, e.g. {meis: [...]}. Take the sole
        # list value rather than guessing at a key name.
        lists = [v for v in parsed.values() if isinstance(v, list)]
        if len(lists) != 1:
            raise ValueError(
                f"{source_file}: expected a list of MEIs, or a mapping with "
                f"exactly one list value; found {len(lists)} list values."
            )
        parsed = lists[0]

    if not isinstance(parsed, list):
        raise ValueError(
            f"{source_file}: expected a list of MEIs, got {type(parsed).__name__}"
        )
    return [str(m).strip() for m in parsed if str(m).strip()]


def build_table_hints(
    table_config: Dict[str, TableSpec],
    explicit_hints: Dict[str, Any],
) -> Dict[str, Any]:
    """Turn per-table index/cursor settings into dlt resource hints.

    ``table_config`` is the declarative front door; ``hints`` remains as
    the escape hatch for anything dlt supports that has no dedicated
    field. Explicit hints win on conflict, so an operator can always
    reach past this function without editing it.

    The generated ``incremental`` is a plain dict, which is what dlt's
    ``apply_hints`` takes for its ``IncrementalArgs`` overload -- verified
    against dlt 1.26 rather than assumed, since a silently-ignored cursor
    would turn every delta load back into a full one.
    """
    hints: Dict[str, Any] = {t: dict(h) for t, h in (explicit_hints or {}).items()}

    for table, spec in table_config.items():
        generated: Dict[str, Any] = {"primary_key": spec.primary_key}
        if spec.cursor:
            incremental: Dict[str, Any] = {"cursor_path": spec.cursor}
            if spec.initial_value is not None:
                incremental["initial_value"] = spec.initial_value
            generated["incremental"] = incremental
        hints[table] = {**generated, **hints.get(table, {})}

    return hints


def latest_completed_query(control: ControlTableSpec) -> str:
    """Timestamp (and load type) of the newest COMPLETED row.

    Written with a MAX() subquery rather than ``ORDER BY ... FETCH FIRST``
    so the same SQL runs on Oracle and on the sqlite stand-in the tests
    use. A tie returns more than one row; the caller takes the first,
    since tied rows carry the same timestamp by definition.
    """
    load_type = f", {control.load_type_column}" if control.load_type_column else ""
    return (
        f"SELECT {control.timestamp_column}{load_type} FROM {control.name} "
        f"WHERE {control.status_column} = :completed_value "
        f"AND {control.timestamp_column} = ("
        f"SELECT MAX({control.timestamp_column}) FROM {control.name} "
        f"WHERE {control.status_column} = :completed_value)"
    )


def latest_done_query(control: ControlTableSpec) -> str:
    """Timestamp of the newest row WE wrote to close a cycle."""
    return (
        f"SELECT MAX({control.timestamp_column}) FROM {control.name} "
        f"WHERE {control.status_column} = :done_value"
    )


def _post_restate(endpoint: str, payload: Dict[str, Any], log) -> None:
    """POST one payload to a Restate ingress.

    Failures are raised, not swallowed. The ack dispatcher logs and
    continues because a missed ack self-heals on the next cycle, but
    these two calls do not: a dropped MEI request means PDM never starts,
    and a dropped completion row means our own sensor re-fires the cycle
    forever.
    """
    response = httpx.post(endpoint, json=payload, timeout=30.0)
    response.raise_for_status()
    log.info(f"Restate accepted payload at {endpoint}")


class RestateDltSyncComponent(Component, Resolvable, Model):
    """Declarative Dagster Component for dlt extraction + Restate acknowledgment.

    For each pipeline it generates, from YAML:
      * the dlt extraction `@multi_asset`
      * a `<pipeline>_<table>_ack_dispatch` `@asset` that reads the ingested
        primary keys back and POSTs them in chunks to a Restate ingress
      * (optional) a `<pipeline>_mei_request` asset that publishes the
        top-level MEI list PDM should explode -- the write that STARTS a
        transaction
      * (optional) a `<pipeline>_load_complete` asset that appends our
        completion row to the control table once every table has landed
      * (optional) a cycle **job** + **sensor**. Given a `control_table:`
        the sensor waits for PDM's COMPLETED row, which is the only signal
        that a load is whole; given only a `backlog_query:` it falls back
        to polling for unprocessed rows.

    The full PDM conversation, when every block is configured::

        mei_request  ->  (PDM explodes MEIs, fills staging, writes
                          STARTED then COMPLETED)
                     ->  sensor sees COMPLETED
                     ->  dlt extract -> ack dispatch -> load_complete
    """

    source_config: Dict[str, Any]
    """The source database/credential configuration."""

    dest_config: Dict[str, Any]
    """The destination system credential configuration."""

    restate_endpoint: str
    """The HTTP endpoint for the Restate service to send ACK chunks."""

    mei_request_endpoint: str = ""
    """Restate ingress for GenericOracleControlService/write_mei_request.
    Required only when a pipeline declares `mei_table:`."""

    load_complete_endpoint: str = ""
    """Restate ingress for GenericOracleControlService/signal_load_complete.
    Required only when a pipeline declares `control_table:`."""

    staging_config: Dict[str, Any] = {}
    """Optional object detailing the staging bucket/filesystem."""

    pipelines: Dict[str, Any] = {}
    """A map of distinct pipeline configurations targeting the Restate handler."""

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Generate the dlt pipelines, their Restate dispatchers, and any
        MEI request / control-table / cycle machinery declared in the YAML."""

        generated_assets: List[Any] = []
        generated_jobs: List[Any] = []
        generated_sensors: List[Any] = []

        for pipeline_key, pipeline_attrs in self.pipelines.items():

            pipeline_attrs = dict(pipeline_attrs)

            sources = pipeline_attrs.pop("sources", [])
            # Pop component-only keys so they aren't passed to DltAssetGroupConfig.
            stats_table = pipeline_attrs.pop("stats_table", None)
            cycle_sensor_cfg = pipeline_attrs.pop("cycle_sensor", None)
            raw_table_config = pipeline_attrs.pop("table_config", None) or {}
            raw_control = pipeline_attrs.pop("control_table", None)
            raw_mei = pipeline_attrs.pop("mei_table", None)

            table_config: Dict[str, TableSpec] = {
                name: TableSpec.model_validate(spec)
                for name, spec in raw_table_config.items()
            }
            unknown_tables = sorted(set(table_config) - set(sources))
            if unknown_tables:
                raise ValueError(
                    f"pipeline '{pipeline_key}': table_config names tables that "
                    f"are not in sources: {unknown_tables}. Either add them to "
                    f"sources or drop the config -- a silently-unused index and "
                    f"cursor reads as configured when it is not."
                )

            control = (
                ControlTableSpec.model_validate(raw_control) if raw_control else None
            )
            mei = MeiTableSpec.model_validate(raw_mei) if raw_mei else None

            if mei and not self.mei_request_endpoint:
                raise ValueError(
                    f"pipeline '{pipeline_key}' declares mei_table but the "
                    f"component has no mei_request_endpoint set."
                )
            if control and not self.load_complete_endpoint:
                raise ValueError(
                    f"pipeline '{pipeline_key}' declares control_table but the "
                    f"component has no load_complete_endpoint set."
                )

            # A single pipeline-wide primary_key remains supported as the
            # default for tables without their own entry.
            default_primary_key = pipeline_attrs.pop("primary_key", None)
            if default_primary_key is None:
                missing = [t for t in sources if t not in table_config]
                if missing:
                    raise ValueError(
                        f"pipeline '{pipeline_key}': no primary_key given for "
                        f"{missing}. Set table_config.<table>.primary_key per "
                        f"table, or a pipeline-level primary_key as the default."
                    )

            pipeline_attrs["hints"] = build_table_hints(
                table_config, pipeline_attrs.pop("hints", None) or {}
            )

            pydantic_config = DltAssetGroupConfig(
                name=pipeline_attrs.pop("name", pipeline_key),
                **pipeline_attrs
            )

            dlt_assets_group = create_dlt_assets(
                sources=sources,
                source_config=self.source_config,
                dest_config=self.dest_config,
                config=pydantic_config,
                staging_config=self.staging_config
            )
            generated_assets.extend(dlt_assets_group)

            # Real dlt asset keys — the cycle job selects all of them; each
            # ack dispatch depends on only its OWN table (see below).
            dlt_keys = _executable_asset_keys(dlt_assets_group)
            dlt_key_for = dlt_key_by_source_table(dlt_assets_group, sources)
            dispatch_keys: List[AssetKey] = []

            for source_table in sources:
                fanout_name = f"{pipeline_key}_{source_table}_ack_dispatch"
                spec = table_config.get(source_table)
                table_pk = spec.primary_key if spec else default_primary_key
                # The ack UPDATE filters on a single column; a composite
                # index cannot be expressed as one IN list.
                if isinstance(table_pk, list):
                    if len(table_pk) != 1:
                        raise ValueError(
                            f"pipeline '{pipeline_key}' table '{source_table}': "
                            f"the acknowledgment update filters on one column, "
                            f"so a composite primary_key {table_pk} cannot be "
                            f"acked. Give the table a single-column key, or "
                            f"drop it from the ack path."
                        )
                    table_pk = table_pk[0]

                def _make_dispatch_asset(
                    _fanout_name, _dlt_deps, _table, _pk, _pydantic_config, _stats_table
                ):
                    @asset(
                        name=_fanout_name,
                        deps=_dlt_deps,
                    )
                    async def dispatch_asset(context):
                        env_credential_name = f"DESTINATION__{self.dest_config.get('drivername', 'POSTGRES').upper()}__CREDENTIALS"
                        pg_url = os.environ.get(env_credential_name)

                        if not pg_url:
                            pg_url = os.environ.get("DESTINATION__POSTGRES__CREDENTIALS")

                        if not pg_url:
                            raise ValueError(f"Missing {env_credential_name} credential for Restate ACK read-back.")

                        engine = sa.create_engine(pg_url)

                        dest_schema_name = getattr(_pydantic_config, "dest_schema", None) or self.source_config.get("schema", "public")
                        query = f"SELECT {_pk} FROM {dest_schema_name}.{_table}"

                        record_ids = []
                        with engine.connect() as conn:
                            result = conn.execute(sa.text(query))
                            record_ids = [row[0] for row in result]

                        context.log.info(f"Retrieved {len(record_ids)} records from destination table {dest_schema_name}.{_table}.")

                        chunk_size = 10000
                        chunks = [record_ids[i:i + chunk_size] for i in range(0, len(record_ids), chunk_size)]

                        async with httpx.AsyncClient() as client:
                            for chunk in chunks:
                                payload = {
                                    "table_name": _table,
                                    "pk_column": _pk,
                                    "record_ids": chunk,
                                }
                                if _stats_table:
                                    payload["stats_table"] = _stats_table

                                context.log.info(f"Dispatching chunk of {len(chunk)} records to Restate ingress.")
                                try:
                                    await client.post(self.restate_endpoint, json=payload)
                                except Exception as e:
                                    context.log.warning(f"Failed to dispatch chunk to Restate: {e}")

                    return dispatch_asset

                # Only this table's dlt output. Falling back to the whole
                # set when unmatched keeps the dispatch downstream of the
                # extraction, which matters more than a tidy graph.
                own_key = dlt_key_for.get(source_table)
                dispatch_deps = [own_key] if own_key else dlt_keys

                dispatch = _make_dispatch_asset(
                    fanout_name, dispatch_deps, source_table, table_pk,
                    pydantic_config, stats_table,
                )
                generated_assets.append(dispatch)
                dispatch_keys.extend(dispatch.keys)

            # ---- MEI request: the write that starts a transaction --------------
            if mei:
                generated_assets.append(
                    self._make_mei_asset(f"{pipeline_key}_mei_request", mei)
                )
                generated_jobs.append(define_asset_job(
                    name=f"{pipeline_key}_mei_request_job",
                    selection=[AssetKey([f"{pipeline_key}_mei_request"])],
                ))
                if mei.source_file:
                    generated_sensors.append(self._make_overlay_sensor(
                        name=f"{pipeline_key}_mei_overlay_sensor",
                        job_name=f"{pipeline_key}_mei_request_job",
                        source_file=mei.source_file,
                    ))

            # ---- load_complete: our row in the control table -------------------
            complete_keys: List[AssetKey] = []
            if control:
                complete = self._make_complete_asset(
                    f"{pipeline_key}_load_complete", control, dispatch_keys,
                )
                generated_assets.append(complete)
                complete_keys.extend(complete.keys)

            # ---- optional cycle job + sensor -----------------------------------
            if cycle_sensor_cfg and cycle_sensor_cfg.get("enabled", True):
                source_url = self.source_config.get("credentials")
                if not source_url:
                    raise ValueError(
                        f"cycle_sensor for pipeline '{pipeline_key}' needs "
                        f"source_config.credentials (a SQLAlchemy URL) to poll the source."
                    )
                backlog_query = cycle_sensor_cfg.get("backlog_query")
                if not backlog_query and not control:
                    raise ValueError(
                        f"cycle_sensor for pipeline '{pipeline_key}' requires "
                        f"either a 'control_table' block (preferred -- it waits "
                        f"for PDM's COMPLETED row, the only signal that a load "
                        f"is whole) or a 'backlog_query' returning a scalar "
                        f"count of unprocessed rows."
                    )
                interval = int(cycle_sensor_cfg.get("interval_seconds", 60))
                job_name = f"{pipeline_key}_cycle_job"
                sensor_name = f"{pipeline_key}_cycle_sensor"

                cycle_job = define_asset_job(
                    name=job_name,
                    selection=dlt_keys + dispatch_keys + complete_keys,
                )
                generated_jobs.append(cycle_job)

                if control:
                    generated_sensors.append(self._make_control_sensor(
                        name=sensor_name, job=cycle_job, job_name=job_name,
                        interval=interval, url=source_url, control=control,
                    ))
                else:
                    generated_sensors.append(self._make_backlog_sensor(
                        name=sensor_name, job=cycle_job, job_name=job_name,
                        interval=interval, url=source_url, query=backlog_query,
                    ))

        return Definitions(
            assets=generated_assets,
            jobs=generated_jobs,
            sensors=generated_sensors,
            resources={"dlt": DagsterDltResource()},
        )

    # -- generated assets ---------------------------------------------------

    def _make_mei_asset(self, name: str, mei: MeiTableSpec):
        endpoint = self.mei_request_endpoint

        @asset(name=name)
        def mei_request_asset(context):
            """Publish the MEI list PDM should explode."""
            meis = load_mei_list(mei.source_file, mei.meis)
            if not meis:
                raise ValueError(
                    f"MEI list resolved to zero entries "
                    f"(source_file={mei.source_file!r}). Writing an empty "
                    f"request would clear the MEI table and leave every "
                    f"MEI-scoped table unpopulated; refusing."
                )
            context.log.info(f"Requesting {len(meis)} MEI(s) into {mei.name}")
            _post_restate(endpoint, {
                "table_name": mei.name,
                "mei_column": mei.mei_column,
                "mei_values": meis,
                "replace": mei.replace,
                "extra_columns": mei.extra_columns,
            }, context.log)

        return mei_request_asset

    def _make_complete_asset(self, name: str, control: ControlTableSpec, deps):
        endpoint = self.load_complete_endpoint

        @asset(name=name, deps=deps)
        def load_complete_asset(context):
            """Append our completion row once every table has landed."""
            load_type = context.run.tags.get("pdm/load_type")
            payload: Dict[str, Any] = {
                "table_name": control.name,
                "status_column": control.status_column,
                "status_value": control.consumer_done_value,
                "timestamp_column": control.timestamp_column,
                "extra_columns": control.extra_columns,
            }
            if control.load_type_column and load_type:
                payload["load_type_column"] = control.load_type_column
                payload["load_type"] = load_type
            context.log.info(
                f"Signalling {control.consumer_done_value!r} in {control.name}"
            )
            _post_restate(endpoint, payload, context.log)

        return load_complete_asset

    # -- generated sensors --------------------------------------------------

    @staticmethod
    def _active_run(context, job_name: str) -> bool:
        """True when a run of this job is already in flight.

        Never start a cycle while one is running: dlt would re-extract
        rows whose ack is still pending, and two completion rows would
        land for one load.
        """
        return bool(context.instance.get_runs(
            filters=RunsFilter(
                job_name=job_name,
                statuses=[
                    DagsterRunStatus.QUEUED,
                    DagsterRunStatus.NOT_STARTED,
                    DagsterRunStatus.STARTING,
                    DagsterRunStatus.STARTED,
                ],
            ),
            limit=1,
        ))

    def _make_control_sensor(self, *, name, job, job_name, interval, url, control):
        component = self

        @sensor(name=name, job=job, minimum_interval_seconds=interval)
        def control_sensor(context):
            if component._active_run(context, job_name):
                return SkipReason(f"{job_name} already running")

            engine = sa.create_engine(url)
            try:
                with engine.connect() as conn:
                    completed = conn.execute(
                        sa.text(latest_completed_query(control)),
                        {"completed_value": control.completed_value},
                    ).fetchone()
                    done_at = conn.execute(
                        sa.text(latest_done_query(control)),
                        {"done_value": control.consumer_done_value},
                    ).scalar()
            finally:
                engine.dispose()

            if not completed or completed[0] is None:
                return SkipReason(
                    f"no {control.completed_value!r} row in {control.name} yet"
                )

            completed_at = completed[0]
            load_type = completed[1] if len(completed) > 1 else None

            # Strictly greater: a consumer-done row stamped at the same
            # instant as the COMPLETED it closed must not re-trigger.
            if done_at is not None and not (completed_at > done_at):
                return SkipReason(
                    f"latest {control.completed_value!r} ({completed_at}) already "
                    f"consumed at {done_at}"
                )

            tags = {"pdm/completed_at": str(completed_at)}
            if load_type:
                tags["pdm/load_type"] = str(load_type)
            return RunRequest(run_key=str(completed_at), tags=tags)

        return control_sensor

    def _make_backlog_sensor(self, *, name, job, job_name, interval, url, query):
        component = self

        @sensor(name=name, job=job, minimum_interval_seconds=interval)
        def cycle_sensor(context):
            if component._active_run(context, job_name):
                return SkipReason(f"{job_name} already running")

            engine = sa.create_engine(url)
            try:
                with engine.connect() as conn:
                    backlog = conn.execute(sa.text(query)).scalar() or 0
            finally:
                engine.dispose()

            if int(backlog) > 0:
                return RunRequest(tags={"cycle_backlog": str(backlog)})
            return SkipReason("no unprocessed rows in source")

        return cycle_sensor

    def _make_overlay_sensor(self, *, name, job_name, source_file):
        @sensor(name=name, job_name=job_name, minimum_interval_seconds=60)
        def mei_overlay_sensor(context):
            """Re-request MEIs when the git overlay changes.

            Keyed on a hash of the resolved list, not the file's mtime or
            bytes: a redeploy rewrites the file and a reformat changes its
            bytes, and neither is a new request. Only a different set of
            MEIs is.
            """
            try:
                meis = load_mei_list(source_file, [])
            except FileNotFoundError as e:
                return SkipReason(str(e))
            if not meis:
                return SkipReason(
                    f"MEI overlay {source_file} resolved to zero entries"
                )

            digest = hashlib.sha256(
                "\n".join(sorted(meis)).encode("utf-8")
            ).hexdigest()[:16]
            if context.cursor == digest:
                return SkipReason(f"MEI list unchanged ({len(meis)} entries)")

            context.update_cursor(digest)
            return RunRequest(run_key=digest, tags={"pdm/mei_count": str(len(meis))})

        return mei_overlay_sensor

import os
from typing import Any, Dict, List

import httpx
import sqlalchemy as sa
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


class RestateDltSyncComponent(Component, Resolvable, Model):
    """Declarative Dagster Component for dlt extraction + Restate acknowledgment.

    For each pipeline it generates, from YAML:
      * the dlt extraction `@multi_asset`
      * a `<pipeline>_<table>_ack_dispatch` `@asset` that reads the ingested
        primary keys back and POSTs them in chunks to a Restate ingress
      * (optional) a cycle **job** + **sensor** — when a pipeline declares a
        `cycle_sensor:` block, the component also emits an asset job binding
        the dlt + dispatch assets and a sensor that polls the source for
        unprocessed rows and re-runs that job. Together they drive the
        read -> ack -> cycle loop hands-off.
    """

    source_config: Dict[str, Any]
    """The source database/credential configuration."""

    dest_config: Dict[str, Any]
    """The destination system credential configuration."""

    restate_endpoint: str
    """The HTTP endpoint for the Restate service to send ACK chunks."""

    staging_config: Dict[str, Any] = {}
    """Optional object detailing the staging bucket/filesystem."""

    pipelines: Dict[str, Any] = {}
    """A map of distinct pipeline configurations targeting the Restate handler."""

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Generate the dlt pipelines, their Restate dispatchers, and any
        cycle jobs/sensors declared in the YAML."""

        generated_assets: List[Any] = []
        generated_jobs: List[Any] = []
        generated_sensors: List[Any] = []

        for pipeline_key, pipeline_attrs in self.pipelines.items():

            pipeline_attrs = dict(pipeline_attrs)

            primary_key = pipeline_attrs.pop("primary_key")
            sources = pipeline_attrs.pop("sources", [])
            # Pop component-only keys so they aren't passed to DltAssetGroupConfig.
            stats_table = pipeline_attrs.pop("stats_table", None)
            cycle_sensor_cfg = pipeline_attrs.pop("cycle_sensor", None)

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

            # Real dlt asset keys — used both as the dispatch asset's upstream
            # dependency and as part of the cycle job selection.
            dlt_keys = _executable_asset_keys(dlt_assets_group)
            dispatch_keys: List[AssetKey] = []

            for source_table in sources:
                fanout_name = f"{pipeline_key}_{source_table}_ack_dispatch"

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

                dispatch = _make_dispatch_asset(
                    fanout_name, dlt_keys, source_table, primary_key,
                    pydantic_config, stats_table,
                )
                generated_assets.append(dispatch)
                dispatch_keys.extend(dispatch.keys)

            # ---- optional cycle job + sensor -----------------------------------
            if cycle_sensor_cfg and cycle_sensor_cfg.get("enabled", True):
                backlog_query = cycle_sensor_cfg.get("backlog_query")
                if not backlog_query:
                    raise ValueError(
                        f"cycle_sensor for pipeline '{pipeline_key}' requires a "
                        f"'backlog_query' (a SELECT COUNT(*) ... returning a scalar)."
                    )
                source_url = self.source_config.get("credentials")
                if not source_url:
                    raise ValueError(
                        f"cycle_sensor for pipeline '{pipeline_key}' needs "
                        f"source_config.credentials (a SQLAlchemy URL) to poll the source."
                    )
                interval = int(cycle_sensor_cfg.get("interval_seconds", 60))
                job_name = f"{pipeline_key}_cycle_job"
                sensor_name = f"{pipeline_key}_cycle_sensor"

                cycle_job = define_asset_job(
                    name=job_name,
                    selection=dlt_keys + dispatch_keys,
                )

                def _make_cycle_sensor(_name, _job, _job_name, _interval, _url, _query):
                    @sensor(name=_name, job=_job, minimum_interval_seconds=_interval)
                    def cycle_sensor(context):
                        # Overlap guard: never start a cycle while one is in flight,
                        # otherwise dlt would re-extract rows whose ack is still pending.
                        active = context.instance.get_runs(
                            filters=RunsFilter(
                                job_name=_job_name,
                                statuses=[
                                    DagsterRunStatus.QUEUED,
                                    DagsterRunStatus.NOT_STARTED,
                                    DagsterRunStatus.STARTING,
                                    DagsterRunStatus.STARTED,
                                ],
                            ),
                            limit=1,
                        )
                        if active:
                            return SkipReason(f"{_job_name} already running")

                        engine = sa.create_engine(_url)
                        try:
                            with engine.connect() as conn:
                                backlog = conn.execute(sa.text(_query)).scalar() or 0
                        finally:
                            engine.dispose()

                        if int(backlog) > 0:
                            return RunRequest(tags={"cycle_backlog": str(backlog)})
                        return SkipReason("no unprocessed rows in source")

                    return cycle_sensor

                generated_jobs.append(cycle_job)
                generated_sensors.append(
                    _make_cycle_sensor(
                        sensor_name, cycle_job, job_name, interval,
                        source_url, backlog_query,
                    )
                )

        return Definitions(
            assets=generated_assets,
            jobs=generated_jobs,
            sensors=generated_sensors,
            resources={"dlt": DagsterDltResource()},
        )

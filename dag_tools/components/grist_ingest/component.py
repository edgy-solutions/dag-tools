"""Declarative component: ingest Grist tables and publish them to Postgres.

Wires up, from a single YAML block:

  * a :class:`~dag_tools.resources.grist.GristResource` for reading Grist,
  * a SQL IO manager (:class:`~dag_tools.io_managers.sql.SQLIOManager`)
    that writes each ingested DataFrame to Postgres,
  * one dynamic-partitioned ``@asset`` whose partition key is a friendly
    table name (which the SQL IO manager also uses as the destination
    table name), and
  * a sensor that discovers Grist documents/tables and fires a run per
    changed table.

The friendly partition key is the operator-facing identity everywhere —
the Dagster asset partition and the Postgres table — while the opaque
Grist doc/table ids travel in run config, out of sight.
"""
# NOTE: intentionally NO `from __future__ import annotations` — the ingest
# asset's `config: GristIngestConfig` parameter must be a real class object
# at definition time so Dagster recognizes it as a pythonic Config type;
# string annotations break that resolution.
from typing import Annotated, Any, Dict, Optional

import pandas as pd
from dagster import (
    AddDynamicPartitionsRequest,
    Config,
    DefaultSensorStatus,
    Definitions,
    DynamicPartitionsDefinition,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    asset,
    define_asset_job,
    sensor,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model, Resolver

from dag_tools.io_managers.sql import ConfigurableSQLIOManager, SQLConfig
from dag_tools.resources.grist import GristConfig, GristResource
from .discovery import discover_tables, normalize_identifier


class GristIngestConfig(Config):
    """Per-run op config: the opaque Grist ids the sensor supplies for a
    given friendly partition. Kept out of the partition key on purpose."""

    doc_id: str
    table_id: str


class GristIngestComponent(Component, Resolvable, Model):
    """Ingest Grist tables to Postgres, discovered by a sensor.

    Everything is driven from YAML; the only external inputs are the
    Grist connection and the Postgres destination, both of which accept
    ``{{ env.VAR }}`` templating.
    """

    grist: Dict[str, Any]
    """Grist connection: ``host``, ``org``, ``token`` (+ optional
    ``scheme`` / ``timeout_seconds``)."""

    postgres: Dict[str, Any]
    """Destination SQL config for the IO manager — ``protocol`` (e.g.
    ``postgresql``), ``host``, ``port``, ``database``, ``schema``,
    ``username``, ``password``. Each ingested table is written to
    ``<schema>.<friendly_name>`` (``write_style`` defaults to replace)."""

    name: Annotated[
        Optional[str],
        Resolver.default(description=(
            "Base name for the generated asset / sensor / job / resources. "
            "Defaults to 'grist'. Normalized to a valid identifier."
        )),
    ] = None

    partition_name: Annotated[
        Optional[str],
        Resolver.default(description=(
            "Dynamic partitions definition name. Defaults to '<name>_tables'."
        )),
    ] = None

    include_workspace_in_name: Annotated[
        bool,
        Resolver.default(description=(
            "Prefix friendly table names with the Grist workspace. Turn off "
            "when doc names are already unique and workspace adds noise."
        )),
    ] = True

    minimum_interval_seconds: Annotated[
        int,
        Resolver.default(description="Minimum seconds between sensor evaluations."),
    ] = 60

    default_status: Annotated[
        str,
        Resolver.default(description="Sensor default status: RUNNING or STOPPED."),
    ] = "STOPPED"

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        base = normalize_identifier(self.name or "grist")
        asset_name = f"{base}_ingest"
        sensor_name = f"{base}_sensor"
        job_name = f"{base}_ingest_job"
        partition_name = self.partition_name or f"{base}_tables"
        grist_key = f"{base}_grist_resource"
        io_key = f"{base}_sql_io_manager"

        partitions_def = DynamicPartitionsDefinition(name=partition_name)
        include_workspace = self.include_workspace_in_name

        # Build the Grist resource once. The ASSET pulls it through
        # Dagster's resource system (required_resource_keys) so execution
        # respects config/overrides; the SENSOR closes over this same
        # instance directly — sensor evaluation only needs to read Grist,
        # and this keeps the sensor independent of resource injection
        # (which also makes it trivially unit-testable).
        grist_resource = GristResource(config=GristConfig.model_validate(self.grist))

        @asset(
            name=asset_name,
            compute_kind="pandas",
            io_manager_key=io_key,
            partitions_def=partitions_def,
            required_resource_keys={grist_key},
        )
        def grist_ingest(context, config: GristIngestConfig) -> pd.DataFrame:
            grist = getattr(context.resources, grist_key)
            partition = context.partition_key
            context.log.info(
                "grist ingest: partition=%s doc=%s table=%s",
                partition, config.doc_id, config.table_id,
            )
            df = grist.get_client().load_table(config.doc_id, config.table_id, context.log)
            if df is None:
                raise RuntimeError(
                    f"grist: failed to load doc={config.doc_id} table={config.table_id}"
                )
            return df

        # Partitioning is inferred from the selected asset — no need to
        # pass partitions_def (deprecated on define_asset_job).
        ingest_job = define_asset_job(name=job_name, selection=[grist_ingest])

        status = (
            DefaultSensorStatus.RUNNING
            if str(self.default_status).upper() == "RUNNING"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=sensor_name,
            job=ingest_job,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=status,
        )
        def grist_sensor(context: SensorEvaluationContext):
            client = grist_resource.get_client()
            since = context.cursor or None

            # One fetch: drives both cursor advancement (past table-less
            # docs too) and table discovery.
            docs = client.list_docs(since=since)
            if not docs:
                return SkipReason(
                    f"grist: no documents updated since cursor {since!r}."
                )
            new_cursor = max(d.get("updatedAt", "") for d in docs)

            discovered = discover_tables(
                client, docs=docs, include_workspace=include_workspace, log=context.log,
            )
            context.update_cursor(new_cursor)
            if not discovered:
                return SkipReason("grist: updated documents contained no tables.")

            run_requests = [
                RunRequest(
                    run_key=t.run_key,
                    partition_key=t.friendly_name,
                    run_config={
                        "ops": {
                            asset_name: {
                                "config": {"doc_id": t.doc_id, "table_id": t.table_id}
                            }
                        }
                    },
                )
                for t in discovered
            ]
            return SensorResult(
                run_requests=run_requests,
                dynamic_partitions_requests=[
                    AddDynamicPartitionsRequest(
                        partitions_def_name=partition_name,
                        partition_keys=[t.friendly_name for t in discovered],
                    )
                ],
            )

        return Definitions(
            assets=[grist_ingest],
            jobs=[ingest_job],
            sensors=[grist_sensor],
            resources={
                grist_key: grist_resource,
                io_key: ConfigurableSQLIOManager(
                    config=SQLConfig.model_validate(self.postgres)
                ),
            },
        )

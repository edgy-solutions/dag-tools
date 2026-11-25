from typing import Any, Dict, List, Optional

from dagster import AssetsDefinition, Definitions
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from dagster_dlt import DagsterDltResource  # noqa: F401  (re-exported by consumers)
from ..dlt_resource import dlt_resources
from dag_tools.asset_wrappers.dlt_assets_parsing import create_dlt_assets
from dag_tools.asset_wrappers.dlt_assets_factory import (
    DltAssetGroupConfig,
    TableSpec,
    build_table_hints,
)
from dag_tools.asset_wrappers.row_filters import callback_for
from dag_tools.asset_wrappers.source_kinds import LEGACY, resolve_source_kind
from .row_count_check import build_row_count_checks
from .schedule import build_pipeline_schedule


class DltPipelineComponent(Component, Resolvable, Model):
    """A declarative Dagster Component for generating DLT Asset pipelines from YAML.
    
    This component parses a `dlt_pipeline` YAML definition and automatically
    instantiates all necessary DLT wrapped `@multi_asset` resources natively.
    """

    source_config: Dict[str, Any]
    """The source database/credential configuration."""

    dest_config: Dict[str, Any]
    """The destination system credential configuration."""

    staging_config: Optional[Dict[str, Any]] = None
    """Optional object detailing the staging bucket/filesystem."""

    pipelines: Dict[str, Any] = {}
    """A map of distinct pipeline configurations (e.g. fast_refresh, slow_refresh)."""

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Dynamically generates all `@multi_asset` DLT pipelines defined in the component."""
        
        generated_assets = []
        extra: Dict[str, List[Any]] = {
            "jobs": [], "schedules": [], "sensors": [], "asset_checks": [],
        }
        kind = resolve_source_kind(self.source_config)
        source_config = kind.localize(self.source_config, context.path)

        for pipeline_key, pipeline_attrs in self.pipelines.items():
            
            pipeline_attrs = dict(pipeline_attrs)
            sources = pipeline_attrs.pop("sources", [])
            # These four are component-only keys and MUST be popped before
            # DltAssetGroupConfig, which now refuses unknown keys: an unpopped
            # one fails the load rather than silently disabling its feature.
            # That is the trade this pop exists for -- before extra="forbid" a
            # schedule left in here vanished without an error, a table_config
            # left in loaded the pipeline with no cursor at all (a full reload
            # every run, reported as success), and a row_filters left in
            # extracted every row of every table while the YAML read as
            # filtered. A new YAML key gets its pop in the same commit.
            schedule = pipeline_attrs.pop("schedule", None)
            raw_table_config = pipeline_attrs.pop("table_config", None) or {}
            raw_row_filters = pipeline_attrs.pop("row_filters", None) or {}

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
            if table_config:
                pipeline_attrs["hints"] = build_table_hints(
                    table_config, pipeline_attrs.pop("hints", None) or {}
                )
            query_callback = callback_for(
                pipeline_key, raw_row_filters, sources,
                builds_sql_query=(
                    kind is LEGACY
                    and self.source_config.get("drivername") != "filesystem"
                ),
                source_description=(
                    f"type {self.source_config.get('type')!r}, "
                    f"driver {self.source_config.get('drivername')!r}"
                ),
            )

            pydantic_config = DltAssetGroupConfig(
                name=pipeline_attrs.pop("name", pipeline_key),
                **pipeline_attrs
            )

            assets = create_dlt_assets(
                sources=sources,
                source_config=source_config,
                dest_config=self.dest_config,
                config=pydantic_config,
                staging_config=self.staging_config,
                query_callback=query_callback,
            )
            generated_assets.extend(assets)

            asset_keys = [
                key for a in assets if isinstance(a, AssetsDefinition) for key in a.keys
            ]
            contributions = [
                kind.extra_defs(
                    pydantic_config.name, asset_keys, source_config, pydantic_config
                )
            ]
            if schedule is not None:
                contributions.append(
                    build_pipeline_schedule(
                        pydantic_config.name, asset_keys, schedule,
                        job_name=kind.schedule_job_name(pydantic_config.name),
                    )
                )
            cursorless = [t for t, spec in table_config.items() if not spec.cursor]
            if cursorless:
                contributions.append({
                    "asset_checks": build_row_count_checks(
                        pydantic_config.name, cursorless, asset_keys
                    )
                })
            for contribution in contributions:
                for field, items in (contribution or {}).items():
                    extra[field].extend(items)

        return Definitions(
            assets=generated_assets,
            resources=dlt_resources(),
            jobs=extra["jobs"],
            schedules=extra["schedules"],
            sensors=extra["sensors"],
            asset_checks=extra["asset_checks"],
        )

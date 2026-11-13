from typing import Any, Dict, List, Optional

from dagster import AssetsDefinition, Definitions
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from dagster_dlt import DagsterDltResource  # noqa: F401  (re-exported by consumers)
from ..dlt_resource import dlt_resources
from dag_tools.asset_wrappers.dlt_assets_parsing import create_dlt_assets
from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig
from dag_tools.asset_wrappers.source_kinds import resolve_source_kind
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

        for pipeline_key, pipeline_attrs in self.pipelines.items():
            
            pipeline_attrs = dict(pipeline_attrs)
            sources = pipeline_attrs.pop("sources", [])
            # Popped before DltAssetGroupConfig: it ignores unknown keys, so a
            # schedule left in would vanish without an error.
            schedule = pipeline_attrs.pop("schedule", None)
            
            pydantic_config = DltAssetGroupConfig(
                name=pipeline_attrs.pop("name", pipeline_key),
                **pipeline_attrs
            )

            assets = create_dlt_assets(
                sources=sources,
                source_config=self.source_config,
                dest_config=self.dest_config,
                config=pydantic_config,
                staging_config=self.staging_config
            )
            generated_assets.extend(assets)

            asset_keys = [
                key for a in assets if isinstance(a, AssetsDefinition) for key in a.keys
            ]
            contributions = [
                kind.extra_defs(
                    pydantic_config.name, asset_keys, self.source_config, pydantic_config
                )
            ]
            if schedule is not None:
                contributions.append(
                    build_pipeline_schedule(pydantic_config.name, asset_keys, schedule)
                )
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

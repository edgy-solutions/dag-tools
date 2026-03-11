from typing import Any, Dict, List, Optional

from dagster import Definitions
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from dag_tools.asset_wrappers.dlt_assets_parsing import create_dlt_assets
from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig


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

        for pipeline_key, pipeline_attrs in self.pipelines.items():
            
            pipeline_attrs = dict(pipeline_attrs)
            sources = pipeline_attrs.pop("sources", [])
            
            pydantic_config = DltAssetGroupConfig(
                name=pipeline_attrs.get("name", pipeline_key),
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
            
        return Definitions(assets=generated_assets)

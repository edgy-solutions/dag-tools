from typing import Any, Dict, List
from dagster_components import Component, ComponentLoadContext
from dagster import Definitions

from dag_tools.asset_wrappers.dlt_assets_parsing import create_dlt_assets
from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig
from .schema import DltPipelineSchema


class DltPipelineComponent(Component):
    """A declarative Dagster Component for generating DLT Asset pipelines from YAML.
    
    This component parses a `dlt_pipeline` YAML definition and automatically
    instantiates all necessary DLT wrapped `@multi_asset` resources natively.
    """

    class ComponentDefinition(ComponentDefinition):
        @classmethod
        def get_schema(cls) -> type[DltPipelineSchema]:
            return DltPipelineSchema

    def __init__(self, dirpath: str, resource_dict: Dict[str, Any]):
        # We manually store the loaded dictionary layout to pass to the factory
        self.dirpath = dirpath
        self.config_dict = resource_dict

    @classmethod
    def load(cls, context: ComponentLoadContext) -> "DltPipelineComponent":
        """Loads the raw loaded Component dictionary into our runtime Component structure."""
        return cls(dirpath=context.decl_node.path, resource_dict=context.decl_node.decl_params)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Dynamically generates all `@multi_asset` DLT pipelines defined in the component."""
        
        # 1. Parse the global configuration block
        attributes = self.config_dict.get("attributes", {})
        
        source_config = attributes.get("source_config", {})
        dest_config = attributes.get("dest_config", {})
        staging_config = attributes.get("staging_config", {})
        pipelines = attributes.get("pipelines", {})
        
        generated_assets = []

        # 2. Iterate through the declared groups and feed them into the DLT factory
        for pipeline_key, pipeline_attrs in pipelines.items():
            
            # Extract the raw table sources for this sub-pipeline
            sources = pipeline_attrs.pop("sources", [])
            
            # Pack the rest into the DLT parser Config
            pydantic_config = DltAssetGroupConfig(
                name=pipeline_attrs.get("name", pipeline_key), # Default name to key if not specified
                **pipeline_attrs
            )

            # 3. Instantiate and append
            assets = create_dlt_assets(
                sources=sources,
                source_config=source_config,
                dest_config=dest_config,
                config=pydantic_config,
                staging_config=staging_config
            )
            generated_assets.extend(assets)
            
        return Definitions(assets=generated_assets)

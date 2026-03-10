import os
from typing import Any, Dict
import httpx
import sqlalchemy as sa
from dagster.components import Component, ComponentLoadContext
from dagster import Definitions, asset

from dag_tools.asset_wrappers.dlt_assets_parsing import create_dlt_assets
from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig
from .schema import RestateApiSyncSchema


class RestateApiSyncComponent(Component):
    """A declarative Dagster Component for generating DLT Asset pipelines from YAML
    that automatically fan-out ingested rows to a Restate ingress service 
    for API synchronization down to the individual record level.
    """

    class ComponentDefinition(ComponentDefinition):
        @classmethod
        def get_schema(cls) -> type[RestateApiSyncSchema]:
            return RestateApiSyncSchema

    def __init__(self, dirpath: str, resource_dict: Dict[str, Any]):
        self.dirpath = dirpath
        self.config_dict = resource_dict

    @classmethod
    def load(cls, context: ComponentLoadContext) -> "RestateApiSyncComponent":
        """Loads the raw loaded Component dictionary into our runtime Component structure."""
        return cls(dirpath=context.decl_node.path, resource_dict=context.decl_node.decl_params)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Dynamically generates all `@multi_asset` DLT pipelines alongside their bound Restate Dispatchers."""
        
        attributes = self.config_dict.get("attributes", {})
        
        source_config = attributes.get("source_config", {})
        dest_config = attributes.get("dest_config", {})
        staging_config = attributes.get("staging_config", {})
        restate_endpoint = attributes.get("restate_endpoint")
        pipelines = attributes.get("pipelines", {})
        
        generated_assets = []

        for pipeline_key, pipeline_attrs in pipelines.items():
            
            # The Restate Fan-Out target PK and API explicit route mappings
            primary_key = pipeline_attrs.pop("primary_key")
            api_path = pipeline_attrs.pop("api_path")
            sources = pipeline_attrs.pop("sources", [])
            
            # Pass through to the underlying DAG tools internal unified DltAssetGroupConfig
            pydantic_config = DltAssetGroupConfig(
                name=pipeline_attrs.get("name", pipeline_key),
                **pipeline_attrs
            )

            # 1. Instantiate the Native DAG tools DLT Extractor mapped uniquely back to our unified hints
            dlt_assets_group = create_dlt_assets(
                sources=sources,
                source_config=source_config,
                dest_config=dest_config,
                config=pydantic_config,
                staging_config=staging_config
            )
            generated_assets.extend(dlt_assets_group)
            
            # 2. Iterate each source table in the group to build a row-level dispatched constraint dynamically
            for source_table in sources:
                fanout_name = f"{pipeline_key}_{source_table}_api_dispatch"
                dlt_asset_dep_str = f"dlt_{pydantic_config.name}_{source_config.get('schema', '')}_asset"
                
                @asset(
                    name=fanout_name,
                    deps=[dlt_asset_dep_str]
                )
                async def dispatch_asset(context, current_table=source_table, pk=primary_key, api_route=api_path):
                    # Query the destination Postgres database staging layer for the newly inserted rows
                    env_credential_name = f"DESTINATION__{dest_config.get('drivername', 'POSTGRES').upper()}__CREDENTIALS"
                    pg_url = os.environ.get(env_credential_name)
                    
                    if not pg_url:
                        pg_url = os.environ.get("DESTINATION__POSTGRES__CREDENTIALS")
                        
                    if not pg_url:
                        raise ValueError(f"Missing {env_credential_name} credential for Restate ACK read-back.")
                    
                    engine = sa.create_engine(pg_url)
                    
                    dest_schema_name = getattr(pydantic_config, "dest_schema", None) or source_config.get("schema", "public")
                    query = f"SELECT * FROM {dest_schema_name}.{current_table}"
                    
                    rows = []
                    with engine.connect() as conn:
                        result = conn.execute(sa.text(query))
                        # Materialize results as dicts for serialization into Restate
                        rows = [dict(row) for row in result.mappings()]
                        
                    context.log.info(f"Retrieved {len(rows)} staged records from {dest_schema_name}.{current_table}.")

                    # Row-Level Fan-Out: Fan out identically to Restate HTTP overhead per record
                    async with httpx.AsyncClient() as client:
                        for idx, row_dict in enumerate(rows):
                            payload = {
                                "api_path": api_route,
                                "source_table": current_table,
                                "pk_column": pk,
                                "pk_value": row_dict.get(pk),
                                "row_data": row_dict
                            }
                            
                            # Logging conservatively to avoid IO drowning for large datasets
                            if idx % 1000 == 0:
                                context.log.info(f"Dispatching record {idx} to Restate /send ingress.")
                                
                            try:
                                # Async fire-and-forget POST to restate_endpoint /send ingress 
                                await client.post(restate_endpoint, json=payload)
                            except Exception as e:
                                context.log.warning(f"Failed to dispatch record PK {row_dict.get(pk)} to Restate: {e}")

                generated_assets.append(dispatch_asset)

        return Definitions(assets=generated_assets)

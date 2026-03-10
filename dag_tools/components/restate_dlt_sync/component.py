import os
from typing import Any, Dict, List
import httpx
import sqlalchemy as sa
from dagster.components import Component, ComponentLoadContext
from dagster import Definitions, asset

from dag_tools.asset_wrappers.dlt_assets_parsing import create_dlt_assets
from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig
from .schema import RestateDltSyncSchema


class RestateDltSyncComponent(Component):
    """A declarative Dagster Component for generating DLT Asset pipelines from YAML
    that automatically fan-out ingested primary keys to a Restate ingress service.
    """

    class ComponentDefinition(ComponentDefinition):
        @classmethod
        def get_schema(cls) -> type[RestateDltSyncSchema]:
            return RestateDltSyncSchema

    def __init__(self, dirpath: str, resource_dict: Dict[str, Any]):
        self.dirpath = dirpath
        self.config_dict = resource_dict

    @classmethod
    def load(cls, context: ComponentLoadContext) -> "RestateDltSyncComponent":
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
            
            # The Restate Fan-Out target PK
            primary_key = pipeline_attrs.pop("primary_key")
            sources = pipeline_attrs.pop("sources", [])
            
            # Hack: Pass through to the underlying DAG tools internal unified DltAssetGroupConfig
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
            
            # 2. Iterate each source table in the group to build a 1:1 dispatched constraint dynamically
            for source_table in sources:
                fanout_name = f"{pipeline_key}_{source_table}_ack_dispatch"
                
                # We need to compute the Dagster AssetKey name depending on DLT prefix logic generated
                # CustomDagsterDltTranslator typically prefixes: `dlt_pipelinename_schema_asset`
                # To be completely safe and avoid explicit string building, we map the dependency to 
                # whatever explicit names the user assigned in Pydantic. If none, we fall back to pipeline name + table
                dlt_asset_dep_str = f"dlt_{pydantic_config.name}_{source_config.get('schema', '')}_asset"
                
                @asset(
                    name=fanout_name,
                    deps=[dlt_asset_dep_str]
                )
                async def dispatch_asset(context, current_table=source_table, pk=primary_key):
                    # We query the destination Postgres database for the newly inserted IDs
                    env_credential_name = f"DESTINATION__{dest_config.get('drivername', 'POSTGRES').upper()}__CREDENTIALS"
                    pg_url = os.environ.get(env_credential_name)
                    
                    if not pg_url:
                        # Attempt generic fallback if Dagster DEV intercepts weren't injected
                        pg_url = os.environ.get("DESTINATION__POSTGRES__CREDENTIALS")
                        
                    if not pg_url:
                        raise ValueError(f"Missing {env_credential_name} credential for Restate ACK read-back.")
                    
                    engine = sa.create_engine(pg_url)
                    
                    # Target table name logic inherited from internal DltAssetGroupConfig defaults
                    dest_schema_name = getattr(pydantic_config, "dest_schema", None) or source_config.get("schema", "public")
                    query = f"SELECT {pk} FROM {dest_schema_name}.{current_table}"
                    
                    record_ids = []
                    with engine.connect() as conn:
                        result = conn.execute(sa.text(query))
                        record_ids = [row[0] for row in result]
                        
                    context.log.info(f"Retrieved {len(record_ids)} records from destination table {dest_schema_name}.{current_table}.")

                    # Dagster Payload Limit: Chunk the IDs into lists of 10,000 for Restate HTTP overhead
                    chunk_size = 10000
                    chunks = [record_ids[i:i + chunk_size] for i in range(0, len(record_ids), chunk_size)]
                    
                    async with httpx.AsyncClient() as client:
                        for chunk in chunks:
                            payload = {
                                "table_name": current_table,
                                "pk_column": pk,
                                "record_ids": chunk
                            }
                            
                            context.log.info(f"Dispatching chunk of {len(chunk)} records to Restate /send ingress.")
                            try:
                                # Async fire-and-forget POST to restate_endpoint /send ingress
                                await client.post(restate_endpoint, json=payload)
                            except Exception as e:
                                context.log.warning(f"Failed to dispatch chunk to Restate: {e}")

                generated_assets.append(dispatch_asset)

        return Definitions(assets=generated_assets)

import os
from typing import Any, Dict

import httpx
import sqlalchemy as sa
from dagster import Definitions, asset
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from dag_tools.asset_wrappers.dlt_assets_parsing import create_dlt_assets
from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig


class RestateDltSyncComponent(Component, Resolvable, Model):
    """A declarative Dagster Component for generating DLT Asset pipelines from YAML
    that automatically fan-out ingested primary keys to a Restate ingress service.
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
        """Dynamically generates all `@multi_asset` DLT pipelines alongside their bound Restate Dispatchers."""
        
        generated_assets = []

        for pipeline_key, pipeline_attrs in self.pipelines.items():
            
            pipeline_attrs = dict(pipeline_attrs)
            
            primary_key = pipeline_attrs.pop("primary_key")
            sources = pipeline_attrs.pop("sources", [])
            
            pydantic_config = DltAssetGroupConfig(
                name=pipeline_attrs.get("name", pipeline_key),
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
            
            for source_table in sources:
                fanout_name = f"{pipeline_key}_{source_table}_ack_dispatch"
                dlt_asset_dep_str = f"dlt_{pydantic_config.name}_{self.source_config.get('schema', '')}_asset"
                
                def _make_dispatch_asset(
                    _fanout_name, _dlt_dep, _table, _pk, _pydantic_config
                ):
                    @asset(
                        name=_fanout_name,
                        deps=[_dlt_dep]
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
                                    "record_ids": chunk
                                }
                                
                                context.log.info(f"Dispatching chunk of {len(chunk)} records to Restate /send ingress.")
                                try:
                                    await client.post(self.restate_endpoint, json=payload)
                                except Exception as e:
                                    context.log.warning(f"Failed to dispatch chunk to Restate: {e}")

                    return dispatch_asset

                generated_assets.append(
                    _make_dispatch_asset(fanout_name, dlt_asset_dep_str, source_table, primary_key, pydantic_config)
                )

        return Definitions(assets=generated_assets)

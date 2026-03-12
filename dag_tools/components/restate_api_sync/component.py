import os
from typing import Any, Dict

import httpx
import sqlalchemy as sa
from dagster import AssetsDefinition, Definitions, AssetKey, asset
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from dagster_dlt import DagsterDltResource
from dag_tools.asset_wrappers.dlt_assets_parsing import create_dlt_assets
from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig


class RestateApiSyncComponent(Component, Resolvable, Model):
    """A declarative Dagster Component for generating DLT Asset pipelines from YAML
    that automatically fan-out ingested rows to a Restate ingress service 
    for API synchronization down to the individual record level.
    """

    source_config: Dict[str, Any]
    """The source database/credential configuration."""

    dest_config: Dict[str, Any]
    """The destination system credential configuration."""

    restate_endpoint: str
    """The HTTP endpoint for the generic Restate service to send rows to."""

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
            api_path = pipeline_attrs.pop("api_path")
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
            
            # Helper to find the correct AssetKey in the DLT multi-asset group
            def _find_dlt_asset_key(table_name):
                # Search through all generated DLT assets in this group
                for dlt_group in dlt_assets_group:
                    if isinstance(dlt_group, AssetsDefinition):
                        for key in dlt_group.keys:
                            if key.path[-1].lower() == table_name.lower():
                                return key
                return None

            for source_table in sources:
                fanout_name = f"{pipeline_key}_{source_table}_api_dispatch"
                
                # Dynamically resolve the upstream AssetKey instead of guessing a string
                upstream_key = _find_dlt_asset_key(source_table)
                if not upstream_key:
                    # Fallback to a sensible default if not found (unlikely)
                    upstream_key = f"dlt_{pydantic_config.name}_{self.source_config.get('schema', '')}_asset"

                def _make_dispatch_asset(
                    _fanout_name, _upstream_key, _table, _pk, _api_route, _pydantic_config
                ):
                    @asset(
                        name=_fanout_name,
                        deps=[_upstream_key]
                    )
                    async def dispatch_asset(context):
                        env_credential_name = f"DESTINATION__{self.dest_config.get('drivername', 'POSTGRES').upper()}__CREDENTIALS"
                        pg_url = os.environ.get(env_credential_name)
                        
                        if not pg_url:
                            pg_url = os.environ.get("DESTINATION__POSTGRES__CREDENTIALS")
                            
                        if not pg_url:
                            raise ValueError(f"Missing {env_credential_name} credential for Restate ACK read-back.")
                        
                        engine = sa.create_engine(pg_url)
                        
                        # Correct logic: Use dest_config schema first, then source_config schema, then public
                        dest_schema_name = (
                            getattr(_pydantic_config, "dest_schema", None) 
                            or self.dest_config.get("schema") 
                            or self.source_config.get("schema") 
                            or "public"
                        )
                        # DLT normalizes table names to lower case by default. 
                        # Postgres quoted identifiers are case-sensitive.
                        normalized_table = _table.lower()
                        query = f'SELECT * FROM "{dest_schema_name}"."{normalized_table}"'
                        
                        rows = []
                        with engine.connect() as conn:
                            result = conn.execute(sa.text(query))
                            rows = [dict(row) for row in result.mappings()]
                            
                        context.log.info(f"Retrieved {len(rows)} staged records from {dest_schema_name}.{normalized_table}.")

                        # Helper to make types like Decimal JSON-serializable
                        def _json_serializable(obj):
                            import decimal
                            if isinstance(obj, dict):
                                return {k: _json_serializable(v) for k, v in obj.items()}
                            elif isinstance(obj, list):
                                return [_json_serializable(v) for v in obj]
                            elif isinstance(obj, decimal.Decimal):
                                return float(obj)
                            return obj

                        # Determine if we are in a container to resolve the correct Restate Ingress URL
                        is_in_container = os.path.exists("/.dockerenv") or os.environ.get("KUBERNETES_SERVICE_HOST")
                        resolved_endpoint = self.restate_endpoint
                        if resolved_endpoint.startswith("http://localhost") and is_in_container:
                            resolved_endpoint = resolved_endpoint.replace("localhost:8083", "restate:8080")
                            context.log.info(f"Detected container environment, routing to internal Restate: {resolved_endpoint}")
                        else:
                            context.log.info(f"Using Restate endpoint: {resolved_endpoint}")

                        async with httpx.AsyncClient() as client:
                            for idx, row_dict in enumerate(rows):
                                # DLT normalizes column names to lower case.
                                # Ensure we find the PK even if the config has it upper-cased.
                                actual_pk_val = row_dict.get(_pk.lower()) or row_dict.get(_pk)
                                
                                payload = _json_serializable({
                                    "api_path": _api_route,
                                    "source_table": _table,
                                    "pk_column": _pk,
                                    "pk_value": actual_pk_val,
                                    "row_data": row_dict
                                })
                                
                                if idx % 1000 == 0:
                                    context.log.info(f"Dispatching record {idx} (PK: {actual_pk_val}) to Restate /send ingress.")
                                    
                                try:
                                    await client.post(resolved_endpoint, json=payload)
                                except Exception as e:
                                    context.log.warning(f"Failed to dispatch record PK {actual_pk_val} to Restate at {resolved_endpoint}: {e}")

                    return dispatch_asset

                generated_assets.append(
                    _make_dispatch_asset(fanout_name, upstream_key, source_table, primary_key, api_path, pydantic_config)
                )

        return Definitions(
            assets=generated_assets,
            resources={"dlt": DagsterDltResource()}
        )

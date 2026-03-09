from dataclasses import dataclass, field

from typing import Annotated, Any, Dict, List, Optional, Sequence
import dagster as dg
from dagster import AssetKey, DagsterEventType, EnvVar, RunStatusSensorContext
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.model import Resolver
from dag_tools.utils.translation_registry import AssetNormalizationRegistry

# External imports required for Datahub Integration
try:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.utilities.urns.dataset_urn import DatasetUrn
    from datahub_dagster_plugin.client.dagster_generator import Constant, DagsterGenerator, DatasetLineage
    from datahub_dagster_plugin.sensors.datahub_sensors import DatahubDagsterSourceConfig
    from datahub_dagster_plugin.sensors.datahub_sensors import make_datahub_sensor as _make_datahub_sensor
    from dagster._core.definitions.events import TextMetadataValue
except ImportError:
    pass

@dataclass
class DatahubLineageComponent(Component):
    """A Dagster Declarative Component that activates global Datahub lineage tracking.
    This component returns exactly 1 definition: the global ASSET_MATERIALIZATION sensor.
    """
    
    datahub_config: Annotated[
        Optional[Dict[str, Any]],
        Resolver.default(description="Datahub ingest configuration (requires 'server').")
    ] = None
    
    environments: Annotated[
        List[str],
        Resolver.default(description="List of environment prefixes in asset keys.")
    ] = field(default_factory=lambda: ["prod", "uat", "sandbox", "dev", "test"])
    
    platforms: Annotated[
        List[str],
        Resolver.default(description="List of platform prefixes in asset keys.")
    ] = field(default_factory=lambda: ["clickhouse", "snowflake", "postgres"])

    filesystem_platforms: Annotated[
        List[str],
        Resolver.default(description="List of platform prefixes that represent file systems.")
    ] = field(default_factory=lambda: ["s3", "abs", "filesystem"])

    log_platform_mappings: Annotated[
        Dict[str, str],
        Resolver.default(description="Mapping of log metadata keys to DataHub platform names.")
    ] = field(default_factory=lambda: {"Databricks Job Run ID": "databricks"})

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        """Constructs and returns the DataHub sensor definition."""
        
        # We wrap the context's Definitions retrieval inside the sensor's execution to prevent initialization loops
        def get_defs():
            # In a unified component context, the definitions are bound to the instance, 
            # but we can resolve the full registry at runtime.
            return context.defs
            
        def asset_keys_to_dataset_urn_converter(
            asset_key: Sequence[str],
            platform: Optional[str] = None
        ) -> Optional[DatasetUrn]:
            """Convert asset key back to dataset urn for generic unmapped lineage."""
            fabric_present = asset_key[0] in self.environments
            platform_value = asset_key[1] if fabric_present else asset_key[0]
            platform_present = platform_value in self.platforms
            
            # Strip prefixed generic ingestion tags dynamically configured in the registry
            known_prefixes = AssetNormalizationRegistry.get_known_asset_prefixes()
            asset_key = asset_key[1:] if asset_key[0] in known_prefixes else asset_key
            asset_key = asset_key[1:] if platform_present else asset_key
            
            platform = platform if platform else platform_value if platform_present else 'unknown'
            
            path = "/".join(asset_key[1:]).lower()
            name = ".".join(asset_key).lower() if platform not in self.filesystem_platforms else f"{asset_key[0].lower()}.{path}"
            
            # Use the specified environment fabric if present, else fallback to 'prod'
            env = asset_key[0] if fabric_present else 'prod'
            
            return DatasetUrn(
                platform=platform,
                env=env,
                name=name,
            )
        
        def asset_lineage_extractor(
            sensor_context: RunStatusSensorContext,
            dagster_generator: DagsterGenerator,
            graph: DataHubGraph,
        ) -> Dict[str, DatasetLineage]:
            
            lineage_map: Dict[str, DatasetLineage] = {}
            defs = sensor_context.instance.get_run_records(filters=dg.RunsFilter(run_ids=[sensor_context.dagster_run.run_id]))[0].run.asset_selection
            
            # Using the run_id to fetch materialization logs 
            logs = sensor_context.instance.all_logs(
                sensor_context.dagster_run.run_id,
                of_type={DagsterEventType.ASSET_MATERIALIZATION},
            )
            
            # Since component `context.defs` is static, we fetch the running definitions via the instance if needed,
            # but usually the registry resolves dynamically. For safety we just use the raw records.
            for log in logs:
                mat = log.asset_materialization
                if not mat:
                    continue
                    
                asset_key_path = mat.asset_key.path
                asset_downstream_urn: Optional[DatasetUrn] = None

                # 1. Check for explicit datahub_urn metadata overrides
                urn_meta = mat.metadata.get("datahub_urn")
                if urn_meta and isinstance(urn_meta, TextMetadataValue):
                    try:
                        asset_downstream_urn = DatasetUrn.from_string(str(urn_meta.text))
                        sensor_context.log.info(f"Resolved URN from metadata: {asset_downstream_urn}")
                    except Exception as e:
                        sensor_context.log.error(f"Error parsing datahub_urn '{urn_meta.text}': {e}")

                # 2. Fall back to generic rule-based conversion
                if not asset_downstream_urn:
                    platform = 'unknown'
                    if 'destination_name' in mat.metadata:
                        platform = str(mat.metadata['destination_name'].value)
                    else:
                        for log_key, mapped_platform in self.log_platform_mappings.items():
                            if log_key in mat.metadata:
                                platform = mapped_platform
                                break
                        
                    asset_downstream_urn = asset_keys_to_dataset_urn_converter(asset_key_path, platform)
                    sensor_context.log.info(f"Resolved URN from asset key: {asset_downstream_urn}")

                if not asset_downstream_urn:
                    continue

                # Prepare the properties mapping
                properties = {k: str(v.value) for k, v in mat.metadata.items() if hasattr(v, 'value')}

                # Extrapolate dependencies (using safe .get instead of raw index)
                # Note: `constant.DATAHUB_INPUTS` tagging requires pulling the definition spec, 
                # but currently we don't have direct access to the asset object from the log.
                # If you use the native metadata inputs, you can parse it from `mat.metadata` directly.
                upstreams_uris = []
                if 'datahub.inputs' in mat.metadata and isinstance(mat.metadata['datahub.inputs'], TextMetadataValue):
                    upstreams_uris.append(mat.metadata['datahub.inputs'].value)
                elif 'datahub.inputs' in properties:
                    pass

                sensor_context.log.info(f"Emitting asset {asset_key_path} to DataHub graph.")
                dagster_generator.emit_asset(
                    graph,
                    asset_key_path,
                    mat.description if mat.description else None,
                    properties,
                    downstreams={asset_downstream_urn.urn()},
                    upstreams=upstreams_uris if upstreams_uris else None,
                    materialize_dependencies=dagster_generator.config.materialize_dependencies,
                )
                
                # FIX: Properly return the lineage mapping safely instead of discarding it silently
                lineage_map[asset_downstream_urn.urn()] = DatasetLineage(
                    upstream_urns=upstreams_uris
                )

            return lineage_map

        # Build Datahub Config dynamically
        server_val = self.datahub_config.get("server", "")
        resolved_server = server_val.get_value() if isinstance(server_val, EnvVar) else server_val
        
        base_config_params = {"server": resolved_server}

        # Handle integration
        dh_config = DatahubDagsterSourceConfig.model_validate(base_config_params)
        
        # Override the defaults
        dh_config = dh_config.model_copy(update={
            'asset_keys_to_dataset_urn_converter': asset_keys_to_dataset_urn_converter, 
            'asset_lineage_extractor': asset_lineage_extractor, 
            'capture_asset_materialization': False
        })
        
        # Build the physical sensor loop
        sensor_def = _make_datahub_sensor(config=dh_config)
        
        return dg.Definitions(sensors=[sensor_def])

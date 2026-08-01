from __future__ import annotations

import os
from dataclasses import dataclass, field

from typing import Annotated, Any, Dict, List, Optional, Sequence
import dagster as dg
from dagster import AssetKey, DagsterEventType, EnvVar, RunStatusSensorContext
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model, Resolver
from dag_tools.utils.translation_registry import AssetNormalizationRegistry

# External imports required for Datahub Integration
try:
    from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
    from datahub.utilities.urns.dataset_urn import DatasetUrn
    from datahub_dagster_plugin.client.dagster_generator import Constant, DagsterGenerator, DatasetLineage
    from datahub_dagster_plugin.sensors.datahub_sensors import DatahubDagsterSourceConfig
    from datahub_dagster_plugin.sensors.datahub_sensors import make_datahub_sensor as _make_datahub_sensor
    from dagster._core.definitions.events import TextMetadataValue
except ImportError:
    pass

def asset_keys_to_dataset_urn_converter(
    asset_key: Sequence[str],
    platform: Optional[str] = None,
    environments: Optional[List[str]] = None,
    platforms: Optional[List[str]] = None,
    filesystem_platforms: Optional[List[str]] = None
) -> Optional[DatasetUrn]:
    """Convert asset key back to dataset urn for generic unmapped lineage."""
    environments = environments or ["prod", "uat", "sandbox", "dev", "test"]
    platforms = platforms or ["clickhouse", "snowflake", "postgres"]
    filesystem_platforms = filesystem_platforms or ["s3", "abs", "filesystem"]

    fabric_present = asset_key[0] in environments
    platform_value = asset_key[1] if fabric_present else asset_key[0]
    platform_present = platform_value in platforms
    
    # Strip prefixed generic ingestion tags dynamically configured in the registry
    known_prefixes = AssetNormalizationRegistry.get_known_asset_prefixes()
    asset_key = asset_key[1:] if asset_key[0] in known_prefixes else asset_key
    asset_key = asset_key[1:] if platform_present else asset_key
    
    platform = platform if platform else platform_value if platform_present else 'unknown'
    
    path = "/".join(asset_key[1:]).lower()
    if platform not in filesystem_platforms:
        name = ".".join(asset_key).lower()
    else:
        # Guard the single-segment case: with no trailing path, the old
        # f"{head}.{path}" produced a malformed URN name ending in a dot
        # (e.g. "mesh_demo_customers."), which then became the dataset's
        # permanent identity in the catalog.
        head = asset_key[0].lower()
        name = f"{head}.{path}" if path else head
    
    # Use the specified environment fabric if present, else fallback to 'prod'
    env = asset_key[0] if fabric_present else 'prod'
    
    return DatasetUrn(
        platform=platform,
        env=env,
        name=name,
    )

def get_datahub_metadata(source_keys: Sequence[Sequence[str]], platform: str) -> Dict[str, List[str]]:
    """Generates the metadata dictionary required to tag Dagster assets with upstream Datahub urns."""
    urns = []
    for source_key in source_keys:
        dataset_urn = asset_keys_to_dataset_urn_converter(source_key, platform=platform)
        if dataset_urn:
            urns.append(dataset_urn.urn())
            
    return {"datahub.inputs": urns}

class DatahubLineageComponent(Component, Resolvable, Model):
    """A Dagster Declarative Component that activates global Datahub lineage tracking.
    This component returns exactly 1 definition: the global ASSET_MATERIALIZATION sensor.

    Resolvable + Model lets Dagster load attribute values straight from a
    component.yaml — without those mixins the loader rejects the YAML
    with ``DagsterInvalidDefinitionError: Component is not resolvable
    from YAML``. Dataclass-style ``field(default_factory=...)`` defaults
    work through the Resolver layer; subclasses don't need to change.
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

    default_status: Annotated[
        str,
        Resolver.default(description=(
            "Whether the emitted sensor starts RUNNING or STOPPED. The "
            "underlying make_datahub_sensor defaults to STOPPED, which means "
            "nothing reaches DataHub until someone enables the sensor in the "
            "Dagster UI. Set RUNNING to have catalog registration active as "
            "soon as the code location loads."
        ))
    ] = "STOPPED"

    sensor_name: Annotated[
        Optional[str],
        Resolver.default(description="Override the generated sensor's name.")
    ] = None

    minimum_interval_seconds: Annotated[
        Optional[int],
        Resolver.default(description="Minimum seconds between sensor evaluations.")
    ] = None

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        """Constructs and returns the DataHub sensor definition."""
        
        # We wrap the context's Definitions retrieval inside the sensor's execution to prevent initialization loops
        def get_defs():
            # In a unified component context, the definitions are bound to the instance, 
            # but we can resolve the full registry at runtime.
            return context.defs
            
        def _bound_converter(asset_key: Sequence[str], platform: Optional[str] = None) -> Optional[DatasetUrn]:
            return asset_keys_to_dataset_urn_converter(
                asset_key, 
                platform=platform, 
                environments=self.environments, 
                platforms=self.platforms, 
                filesystem_platforms=self.filesystem_platforms
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
                        
                    asset_downstream_urn = _bound_converter(asset_key_path, platform)
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

        # Build Datahub Config dynamically.
        #
        # DatahubDagsterSourceConfig requires a NESTED datahub_client_config;
        # it rejects a bare ``server`` (extra_forbidden) and errors on the
        # missing nested field. An earlier version passed ``{"server": ...}``
        # flat, so building the sensor always failed pydantic validation —
        # i.e. this component could never actually register anything.
        config_source = self.datahub_config or {}

        def _resolve(value: Any) -> Any:
            return value.get_value() if isinstance(value, EnvVar) else value

        resolved_server = _resolve(config_source.get("server", ""))

        client_params: Dict[str, Any] = {"server": resolved_server}

        # A PAT is required when the metadata service runs with
        # METADATA_SERVICE_AUTH_ENABLED=true (the correct posture). Accept it
        # from config, else the standard DATAHUB_TOKEN env var; None means
        # unauthenticated, which only works against an open GMS.
        token = _resolve(config_source.get("token")) or os.environ.get("DATAHUB_TOKEN")
        if token:
            client_params["token"] = token

        dh_config = DatahubDagsterSourceConfig(
            datahub_client_config=DatahubClientConfig(**client_params)
        )
        
        # Override the defaults
        dh_config = dh_config.model_copy(update={
            'asset_keys_to_dataset_urn_converter': _bound_converter, 
            'asset_lineage_extractor': asset_lineage_extractor, 
            'capture_asset_materialization': False
        })
        
        # Build the physical sensor loop.
        #
        # default_status is passed explicitly: make_datahub_sensor defaults
        # to STOPPED, so without this the sensor is defined but idle and
        # nothing ever reaches DataHub until an operator toggles it in the
        # UI — a silent no-op that looks like a broken integration.
        from dagster import DefaultSensorStatus

        status = (
            DefaultSensorStatus.RUNNING
            if str(self.default_status).upper() == "RUNNING"
            else DefaultSensorStatus.STOPPED
        )

        sensor_kwargs: Dict[str, Any] = {"config": dh_config, "default_status": status}
        if self.sensor_name:
            sensor_kwargs["name"] = self.sensor_name
        if self.minimum_interval_seconds is not None:
            sensor_kwargs["minimum_interval_seconds"] = self.minimum_interval_seconds

        sensor_def = _make_datahub_sensor(**sensor_kwargs)

        return dg.Definitions(sensors=[sensor_def])

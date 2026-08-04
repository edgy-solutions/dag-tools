from __future__ import annotations

import os
from dataclasses import dataclass, field

from typing import Annotated, Any, Dict, List, Optional, Sequence
import dagster as dg
from dagster import (
    AssetKey,
    DagsterEventType,
    EnvVar,
    RunStatusSensorContext,
    TableSchemaMetadataValue,
    # Imported from the PUBLIC namespace, and deliberately not inside the
    # optional-datahub try/except below. It used to come from
    # ``dagster._core.definitions.events``, a private path where the symbol
    # does not actually live — on 1.10.19 or 1.13.16. Because that import
    # sat inside ``except ImportError: pass`` it failed silently, leaving
    # the name unbound, and the ``datahub_urn`` override path raised
    # NameError the moment anyone used it. Nothing surfaced it: the
    # isinstance() guard is short-circuited by `if urn_meta and ...`, so
    # the reference is never evaluated unless an asset actually publishes
    # that metadata.
    TextMetadataValue,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model, Resolver
from dag_tools.components.datahub_lineage.platforms import (
    FILESYSTEM_PLATFORMS,
    UNKNOWN_PLATFORM,
    resolve_platform,
)
from dag_tools.utils.translation_registry import AssetNormalizationRegistry

# External imports required for Datahub Integration
try:
    from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
    from datahub.utilities.urns.dataset_urn import DatasetUrn
    from datahub_dagster_plugin.client.dagster_generator import Constant, DagsterGenerator, DatasetLineage
    from datahub_dagster_plugin.sensors.datahub_sensors import DatahubDagsterSourceConfig
    from datahub_dagster_plugin.sensors.datahub_sensors import make_datahub_sensor as _make_datahub_sensor
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

def _extract_upstream_urns(metadata: Any) -> List[str]:
    """Pull the ``datahub.inputs`` URN strings off a materialization's metadata.

    ``get_datahub_metadata()`` writes a LIST of urn strings, but Dagster wraps
    metadata values in a MetadataValue subclass whose payload lives on
    ``.value`` (JSON/list) or ``.text`` (text). Normalize any of those shapes
    to a flat list of strings, dropping anything that isn't one.
    """
    raw = (metadata or {}).get("datahub.inputs")
    if raw is None:
        return []
    payload = getattr(raw, "value", None)
    if payload is None:
        payload = getattr(raw, "text", None)
    if payload is None:
        payload = raw

    if isinstance(payload, str):
        candidates: List[Any] = [payload]
    elif isinstance(payload, (list, tuple, set)):
        candidates = list(payload)
    else:
        return []

    out: List[str] = []
    for c in candidates:
        # Tolerate one level of nesting (an older writer appended the list itself).
        if isinstance(c, (list, tuple, set)):
            out.extend(str(i) for i in c if isinstance(i, str))
        elif isinstance(c, str):
            out.append(c)
    return out


def _extract_table_schema(metadata: Any) -> Any:
    """The ``TableSchemaMetadataValue`` off a materialization, if any.

    Column-level schema in the catalog used to come from
    ``CortexPolarsIOManager``, which emitted to DataHub directly. That emit
    was removed (an IO manager is bound per-asset and may be bound to
    assets another deployment owns, so it cannot honestly claim
    authorship), and the sensor that replaced it never re-emitted schema --
    so datasets kept their columns as a fossil from the last cortex write
    and new datasets had none at all. Nothing failed; the aspect was just
    silently absent.

    Dagster's conventional key is ``dagster/column_schema`` (what
    dagster-dbt attaches automatically), but the value is accepted under
    any key so an asset can publish it however it likes -- the type is the
    contract, not the name.
    """
    md = metadata or {}
    preferred = md.get("dagster/column_schema")
    if isinstance(preferred, TableSchemaMetadataValue):
        return preferred
    for value in md.values():
        if isinstance(value, TableSchemaMetadataValue):
            return value
    return None


def _emit_physical_only(
    *, graph, generator, urn, description, properties, upstreams, table_schema, log
) -> None:
    """Register the physical dataset as the single entity for an asset.

    Mirrors what ``emit_asset`` puts on its dagster-platform dataset --
    properties, description, subtype, status, schema, upstream lineage --
    but onto the URN a DataHub source crawler will independently discover,
    so the two converge on one entity instead of racing to create two.
    """
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        DatasetLineageTypeClass,
        DatasetPropertiesClass,
        StatusClass,
        SubTypesClass,
        UpstreamClass,
        UpstreamLineageClass,
    )

    urn_str = urn.urn()
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=urn_str,
            aspect=DatasetPropertiesClass(
                description=description,
                customProperties={k: str(v) for k, v in (properties or {}).items()},
            ),
        ),
        MetadataChangeProposalWrapper(entityUrn=urn_str, aspect=StatusClass(removed=False)),
        MetadataChangeProposalWrapper(
            entityUrn=urn_str, aspect=SubTypesClass(typeNames=["Table"])
        ),
    ]
    if upstreams:
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=urn_str,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(dataset=u, type=DatasetLineageTypeClass.TRANSFORMED)
                        for u in upstreams
                    ]
                ),
            )
        )
    if table_schema is not None:
        try:
            mcps.append(
                generator.convert_table_schema_to_schema_metadata(
                    table_schema=table_schema, parent_urn=urn
                )
            )
        except Exception as e:
            log.warning("could not attach schema to %s: %s", urn_str, e)

    for mcp in mcps:
        try:
            graph.emit_mcp(mcp)
        except Exception as e:
            log.warning("emit failed for %s: %s", urn_str, e)


def _record_lineage(lineage_map, step_key, asset_key_path, upstreams, downstream_urn):
    """Same per-step lineage the plugin merges, keyed by step_key."""
    inputs = _to_dataset_urns(upstreams)
    outputs = {downstream_urn} if downstream_urn is not None else set()
    key = step_key or (
        downstream_urn.urn() if downstream_urn is not None else ".".join(asset_key_path)
    )
    if key in lineage_map:
        prior = lineage_map[key]
        lineage_map[key] = DatasetLineage(
            inputs=prior.inputs | inputs, outputs=prior.outputs | outputs
        )
    else:
        lineage_map[key] = DatasetLineage(inputs=inputs, outputs=outputs)


def _physical_urn_for_asset(
    sensor_context: Any, asset_key: Any, converter: Any, platform_of: Any
) -> Optional[Any]:
    """The PHYSICAL dataset URN for an asset, or None if it has no location.

    An asset that materializes an S3 table and the S3 table are the same
    real-world object, so they get ONE catalog entity -- the physical one,
    named exactly as a DataHub source crawler would discover it. Assets
    with no physical location (a staging step, a source stub) keep a
    dagster-platform entity, because there is no table to point at.

    Resolution uses the asset's own last materialization: the platform it
    declared via ``destination_name``, run through the same converter that
    built its URN when it was written. That keeps a parent's identity
    stable across runs rather than re-deriving it from whatever this run
    happens to know.
    """
    try:
        instance = sensor_context.instance
        rec = instance.get_latest_materialization_event(asset_key)
        mat = rec.asset_materialization if rec else None
        if mat is None:
            return None
        declared = mat.metadata.get("destination_name")
        declared = str(declared.value) if declared is not None else None
        if not declared:
            return None
        platform = platform_of(declared)
        if platform == UNKNOWN_PLATFORM:
            return None
        return converter(list(asset_key.path), platform)
    except Exception as e:
        sensor_context.log.warning(
            "could not resolve a physical URN for %s: %s", asset_key, e
        )
        return None


def _graph_upstream_urns(sensor_context: Any, asset_key: Any, generator: Any) -> List[str]:
    """Upstream URNs taken from the Dagster asset graph itself.

    ``datahub.inputs`` metadata only exists on assets that opted in by
    calling ``get_datahub_metadata()``. An asset declaring
    ``deps=[other_key]`` -- or taking an upstream as a function argument --
    has a real edge in the asset graph but publishes no such metadata, so
    lineage for it was silently empty. That is the common case: it is how
    Dagster models dependencies, and nothing warns you that the catalog did
    not record it.

    Reading the graph makes lineage the default rather than an opt-in.
    ``datahub.inputs`` still works and is merged with this, since it can
    name upstreams that live outside Dagster entirely.

    URNs are built with the plugin's own ``dataset_urn_from_asset`` so they
    are byte-identical to the ones ``emit_asset`` creates for those parents
    -- a hand-rolled URN that differs in case or separator produces a
    dangling edge to an entity that does not exist.

    Best-effort: lineage is worth less than the materialization record, so
    any failure here degrades to "no graph lineage" rather than aborting
    the emit for the whole run.
    """
    try:
        repository_def = sensor_context.repository_def
        if repository_def is None:
            return []
        asset_graph = repository_def.asset_graph
        parents = asset_graph.get(asset_key).parent_keys
    except Exception as e:
        sensor_context.log.warning(f"Could not read asset graph for lineage: {e}")
        return []

    urns: List[str] = []
    for parent in parents or []:
        try:
            # Prefer the parent's PHYSICAL urn so lineage links table to
            # table. Falls back to the dagster-platform urn for parents
            # that have no physical location -- a staging step, say --
            # which is the only case where a dagster entity is the real
            # identity rather than a duplicate of one.
            physical = getattr(sensor_context, "_dagtools_physical_resolver", None)
            urn = physical(parent) if physical else None
            urns.append(urn.urn() if urn is not None
                        else generator.dataset_urn_from_asset(parent.path).urn())
        except Exception:
            continue
    return urns


def _to_dataset_urns(urn_strings: Sequence[str]) -> set:
    """Convert URN strings to ``DatasetUrn`` objects, skipping malformed ones.

    ``DatasetLineage`` holds ``Set[DatasetUrn]``, not strings. A single bad
    URN must not abort the emit for every other asset in the run, so parse
    failures are skipped rather than raised.
    """
    urns = set()
    for s in urn_strings or []:
        try:
            urns.add(DatasetUrn.from_string(s))
        except Exception:
            continue
    return urns


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
        Resolver.default(description=(
            "Platforms whose dataset names are laid out as a path rather than "
            "a dotted identifier. Must stay in step with the platform mapping: "
            "the name format is chosen by platform, so a mapped-to name that is "
            "missing here silently changes the dataset's NAME, and a different "
            "name is a different entity."
        ))
    ] = field(default_factory=lambda: list(FILESYSTEM_PLATFORMS))

    log_platform_mappings: Annotated[
        Dict[str, str],
        Resolver.default(description="Mapping of log metadata keys to DataHub platform names.")
    ] = field(default_factory=lambda: {"Databricks Job Run ID": "databricks"})

    emit_dagster_assets: Annotated[
        bool,
        Resolver.default(description=(
            "Emit a dataPlatform:dagster dataset ALONGSIDE the physical one. "
            "Off by default: an asset and the table it writes are one object, "
            "and a second entity never reconciles with what a DataHub source "
            "crawler discovers. Assets with no physical location still get a "
            "dagster entity regardless -- there is no table to point at. Turn "
            "on only to restore the previous two-node behaviour."
        ))
    ] = False

    platform_mappings: Annotated[
        Dict[str, str],
        Resolver.default(description=(
            "Overrides for translating a producer's declared platform into "
            "DataHub's name for it, e.g. {'s3_delta': 'delta-lake'}. Checked "
            "before the built-in tables, so a new backend or a renamed "
            "DataHub platform can be handled from config rather than a "
            "release. See dag_tools.components.datahub_lineage.platforms."
        ))
    ] = field(default_factory=dict)

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

            # NOTE: an earlier version fetched the run's asset_selection here
            # via ``get_run_records(...)[0].run.asset_selection``. That was
            # dead code — the value was never read — and it crashed the whole
            # emit with ``AttributeError: 'RunRecord' object has no attribute
            # 'run'`` (the attribute is ``dagster_run``). Every materialization
            # event we need is already in the logs below, keyed by run_id.

            # Using the run_id to fetch materialization logs
            logs = sensor_context.instance.all_logs(
                sensor_context.dagster_run.run_id,
                of_type={DagsterEventType.ASSET_MATERIALIZATION},
            )

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
                    # The producer names its own platform. It is the only
                    # party that knows what it actually wrote -- inferring
                    # it from asset-key prefixes meant every key had to be
                    # spelled a way the inference recognised, against a
                    # hardcoded list that lived nowhere near the backend.
                    declared = None
                    if 'destination_name' in mat.metadata:
                        declared = str(mat.metadata['destination_name'].value)
                    else:
                        for log_key, mapped_platform in self.log_platform_mappings.items():
                            if log_key in mat.metadata:
                                declared = mapped_platform
                                break

                    # ...but it names it in ITS vocabulary, not DataHub's:
                    # a Delta table on S3 is "s3_delta" to the IO manager
                    # and "delta-lake" to DataHub. See platforms.py.
                    platform = resolve_platform(declared, self.platform_mappings)
                    if platform == UNKNOWN_PLATFORM:
                        # No platform declared, so there is nothing truthful
                        # to say about WHERE this asset lives. Emitting a
                        # physical dataset anyway produced a second entity on
                        # DataHub's "unknown" platform -- a permanent twin of
                        # the real asset, indistinguishable in the UI from a
                        # genuine dataset, created afresh on every run.
                        #
                        # The asset itself is still catalogued below; only
                        # the physical-location half is withheld. That is the
                        # honest split: we know the asset exists, we do not
                        # know what it is stored in.
                        sensor_context.log.info(
                            "Asset %s declared no platform%s; cataloguing the "
                            "asset without a physical dataset.",
                            asset_key_path,
                            f" (got {declared!r})" if declared else "",
                        )
                    else:
                        asset_downstream_urn = _bound_converter(asset_key_path, platform)
                        sensor_context.log.info(
                            f"Resolved URN from asset key: {asset_downstream_urn}"
                        )

                # Prepare the properties mapping
                properties = {k: str(v.value) for k, v in mat.metadata.items() if hasattr(v, 'value')}

                # Upstream URNs come from the ``datahub.inputs`` metadata that
                # get_datahub_metadata() attaches (a LIST of urn strings), so
                # normalize whatever metadata wrapper Dagster used back into a
                # flat list of strings. The previous code only handled
                # TextMetadataValue and appended the raw ``.value`` — which for
                # the list case would have nested a list inside the list.
                upstreams_uris = _extract_upstream_urns(mat.metadata)

                # ...plus whatever the asset graph already knows. Assets
                # normally declare dependencies with deps=[...] or a function
                # argument, neither of which writes datahub.inputs, so without
                # this the catalog records the asset but none of its lineage.
                # Deduplicated because an asset may declare both.
                for urn in _graph_upstream_urns(
                    sensor_context, mat.asset_key, dagster_generator
                ):
                    if urn not in upstreams_uris:
                        upstreams_uris.append(urn)

                # Column-level schema, when the asset published one. Without
                # this the dataset registers with lineage but no columns --
                # which is what the catalog looked like after the cortex IO
                # manager's direct emit was removed and nothing replaced it.
                table_schema = _extract_table_schema(mat.metadata)

                # ONE entity per real table. emit_asset always creates a
                # dataPlatform:dagster dataset and hangs the physical one
                # off it as a downstream, so every asset showed up twice --
                # a rich "Asset" node and a near-empty file node, with the
                # graph reading source -> asset -> file for what is one
                # object. The physical URN is the identity a DataHub source
                # crawler will independently discover, so it is the one
                # that must exist; emitting a second entity guarantees the
                # two never reconcile.
                #
                # Assets with no physical location keep the dagster entity:
                # there is no table to point at, and the node is then the
                # real identity rather than a duplicate.
                if asset_downstream_urn is not None and not self.emit_dagster_assets:
                    _emit_physical_only(
                        graph=graph,
                        generator=dagster_generator,
                        urn=asset_downstream_urn,
                        description=mat.description or None,
                        properties=properties,
                        upstreams=upstreams_uris,
                        table_schema=table_schema,
                        log=sensor_context.log,
                    )
                    _record_lineage(
                        lineage_map, log.step_key, asset_key_path,
                        upstreams_uris, asset_downstream_urn,
                    )
                    continue

                sensor_context.log.info(f"Emitting asset {asset_key_path} to DataHub graph.")
                dagster_generator.emit_asset(
                    graph,
                    asset_key_path,
                    mat.description if mat.description else None,
                    properties,
                    downstreams=(
                        {asset_downstream_urn.urn()} if asset_downstream_urn else None
                    ),
                    upstreams=upstreams_uris if upstreams_uris else None,
                    schema=table_schema,
                    materialize_dependencies=dagster_generator.config.materialize_dependencies,
                )

                # emit_asset attaches the schema to the DAGSTER dataset (the
                # asset entity) only. But the physical dataset -- the s3 /
                # postgres / delta-lake entry -- is the one someone browsing
                # the catalog by platform actually opens, and it was showing
                # lineage with an empty schema tab. Mirror the columns onto
                # it so the physical table describes itself.
                #
                # Best-effort: the asset is already registered by this point,
                # so a failure here costs a schema tab, not the emit.
                if table_schema is not None and asset_downstream_urn is not None:
                    try:
                        graph.emit_mcp(
                            dagster_generator.convert_table_schema_to_schema_metadata(
                                table_schema=table_schema,
                                parent_urn=asset_downstream_urn,
                            )
                        )
                    except Exception as e:
                        sensor_context.log.warning(
                            "Could not attach schema to %s: %s",
                            asset_downstream_urn.urn(), e,
                        )

                # Record the lineage for THIS step so the plugin can merge it
                # with what it derives from the logs itself.
                #
                # DatasetLineage is a NamedTuple(inputs, outputs) of
                # Set[DatasetUrn]. An earlier version called it with
                # ``upstream_urns=`` (a kwarg that does not exist) and passed
                # strings — raising TypeError on the very last statement of
                # this extractor. Because the plugin calls the extractor
                # BEFORE generate_dataflow / emit_job_run, that exception
                # aborted the whole emit: no DataFlow, no DataJob, no
                # DataProcessInstance, and none of the merged lineage.
                #
                # Key by step_key to line up with the plugin's own
                # process_dagster_logs() map (also Dict[str, Set[DatasetUrn]],
                # keyed per step); the two are merged by key.
                inputs = _to_dataset_urns(upstreams_uris)
                # No physical dataset when the platform is unknown, so the
                # step has inputs but no output to record. The lineage entry
                # still matters -- it is what the plugin merges upstream
                # edges from.
                outputs = {asset_downstream_urn} if asset_downstream_urn else set()
                key = log.step_key or (
                    asset_downstream_urn.urn()
                    if asset_downstream_urn
                    else ".".join(asset_key_path)
                )
                if key in lineage_map:
                    prior = lineage_map[key]
                    lineage_map[key] = DatasetLineage(
                        inputs=prior.inputs | inputs,
                        outputs=prior.outputs | outputs,
                    )
                else:
                    lineage_map[key] = DatasetLineage(inputs=inputs, outputs=outputs)

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

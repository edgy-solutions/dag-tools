import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union

import dlt
from dlt.extract.source import DltSource
from dlt.pipeline.pipeline import Pipeline
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetSpec,
    AssetsDefinition,
    BackfillPolicy,
    EnvVar,
    PartitionsDefinition,
    TimeWindowPartitionsDefinition,
    multi_asset,
)
from dagster._check import inst_param
from dagster_embedded_elt.dlt import DagsterDltResource, build_dlt_asset_specs

from dag_tools.asset_wrappers.dlt_assets_factory import (
    CustomDagsterDltTranslator,
    DltAssetGroupConfig,
    ENV_VARS,
    config_to_credentials,
    select_columns_f,
    using_dagster_dev,
    write_env_vars,
    DltAssetConfig,
)
from orch.assets.sources.sql_ct_database import sql_ct_database
from dlt.sources.sql_database import sql_database
from dlt.sources.filesystem import filesystem, read_parquet
from orch.helpers.credential_provider import get_credentials


def add_element(
    mapping: Dict[str, Any],
    database: str,
    schema: str,
    source_path: list | str,
    table: str,
    creds: Any,
    source_config: Dict[str, Any],
) -> None:
    base = mapping.setdefault(database, {}).setdefault(schema, {})
    base.setdefault("source", {})[table] = source_path
    base.setdefault("tables", []).append(table)
    base.setdefault("creds", creds)
    base.setdefault("source_config", source_config)


def explode_orig_naming(database: str, schema: str, table: str) -> tuple[str, str, str]:
    split = "___" if "___" in table or "___" in schema else "__"

    if split in table:
        parts = table.split(split)
        database = parts[-3] if len(parts) > 2 else schema
        schema, table = parts[-2], parts[-1]
    elif split in schema:
        parts = schema.split(split)
        database, schema = parts[-2], parts[-1]

    return database, schema, table


def process_sources(
    sources: List[AssetKey], mapping: Dict[str, Any], source_config: Dict[str, Any]
) -> None:
    for source in sources:
        if len(source.path) < 3:
            print(f"Key {source} is not properly formatted with a database, schema and table")
            continue

        database, schema, table = source.path[-3].lower(), source.path[-2].lower(), source.path[-1].lower()
        creds = config_to_credentials(source_config or get_credentials(source))
        add_element(mapping, database, schema, source.path, table, creds, source_config)


def get_destination(credentials: Any, config: Dict[str, Any] = None, vars: Dict[str, Any] = None, database: str = None) -> Any:
    config = dict(config or {})
    vars = vars or {}

    if using_dagster_dev():
        for key, value in config.items():
            vars[f"DESTINATION__{credentials.drivername.upper()}__{key.upper()}"] = value
        for key, value in credentials.__dict__.items():
            vars[f"DESTINATION__{credentials.drivername.upper()}__CREDENTIALS__{key.upper()}"] = value

    config.setdefault("enable_dataset_name_normalization", False)
    config.setdefault("staging_use_https", False)

    if database and "normalize" in config and database in config["normalize"]:
        for section, cfg in config["normalize"][database].items():
            for key, value in cfg.items():
                os.environ[f"{database.upper()}_PIPELINE__NORMALIZE__{section.upper()}__{key.upper()}"] = value
        del config["normalize"]

    try:
        if credentials.drivername == "postgresql":
            return dlt.destinations.postgres(credentials=credentials, **config)
        elif credentials.drivername == "snowflake":
            return dlt.destinations.snowflake(staging_dataset_name_layout=credentials.staging, credentials=credentials, **config)
        elif credentials.drivername == "filesystem":
            if database and "bucket_url" in config:
                bucket_url = config["bucket_url"]
                base_url = bucket_url.get_value() if isinstance(bucket_url, EnvVar) else bucket_url
                config["bucket_url"] = f"{base_url}/{database}"

            config["destination_name"] = "s3" if credentials.__class__.__name__ == "AwsCredentials" else "abs"
            return dlt.destinations.filesystem(credentials=credentials, **config)
            
        elif credentials.drivername == "clickhouse":
            return dlt.destinations.clickhouse(credentials=credentials, **config)
        elif credentials.drivername == "databricks":
            return dlt.destinations.databricks(credentials=credentials, **config)
            
    except Exception as e:
        print(f"Warning instantiating destination: {e}")

    return "filesystem"


def dlt_assets_with_io_managers(
    *,
    dlt_source: DltSource,
    dlt_pipeline: Pipeline,
    io_manager_key: str,
    name: Optional[str] = None,
    group_name: Optional[str] = None,
    dagster_dlt_translator: Optional[CustomDagsterDltTranslator] = None,
    partitions_def: Optional[PartitionsDefinition] = None,
    backfill_policy: Optional[BackfillPolicy] = None,
    op_tags: Optional[Mapping[str, Any]] = None,
    pool: Optional[str] = None,
    ins: Optional[Mapping[str, AssetIn]] = None,
) -> Callable[[Callable[..., Any]], AssetsDefinition]:
    dagster_dlt_translator = inst_param(
        dagster_dlt_translator or CustomDagsterDltTranslator({}, "", "", "", ""),
        "dagster_dlt_translator", CustomDagsterDltTranslator
    )

    if partitions_def and isinstance(partitions_def, TimeWindowPartitionsDefinition) and not backfill_policy:
        backfill_policy = BackfillPolicy.single_run()

    specs = build_dlt_asset_specs(
        dlt_source=dlt_source,
        dlt_pipeline=dlt_pipeline,
        dagster_dlt_translator=dagster_dlt_translator,
    )

    return multi_asset(
        name=name,
        group_name=group_name,
        can_subset=True,
        partitions_def=partitions_def,
        backfill_policy=backfill_policy,
        op_tags=op_tags,
        specs=[spec.with_io_manager_key(io_manager_key) for spec in specs],
        pool=pool,
        ins=ins,
    )


def instantiate_assets(
    base: Dict[str, Any],
    database: str,
    schema: str,
    query_callback: Optional[Callable],
    dlt_pipeline: Pipeline,
    dest_database: str,
    dest_schema: str,
    dest_driver: str,
    kinds: List[str],
    config: DltAssetGroupConfig,
    default_pipeline_kwargs: Dict[str, Any],
    defer_table_reflect: bool = True,
) -> AssetsDefinition:
    """Dynamically builds DLT generator source and returns a mapped Dagster `@multi_asset` using GroupConfig."""
    
    if base["creds"].drivername == "filesystem":
        @dlt.source
        def filesystem_source():
            local_hints = config.hints.copy()
            for table in base["tables"]:
                bucket_path = f"{base['source_config']['destination']['bucket_url']}/{database.replace('.','_')}/{schema}/{table}"
                
                fs_kwargs = {}
                if table in local_hints and "incremental" in local_hints[table]:
                    inc_hint = local_hints[table]["incremental"]
                    if getattr(inc_hint, "cursor_path", None) == "modification_date":
                        fs_kwargs["incremental"] = inc_hint
                        del local_hints[table]["incremental"]

                res = (
                    filesystem(
                        credentials=base["creds"],
                        bucket_url=bucket_path,
                        file_glob="*.parquet",
                        **fs_kwargs,
                    ) | read_parquet()
                )
                yield res.with_name(table)

        source = filesystem_source()
    else:
        func = sql_ct_database if "mssql" in base["creds"].drivername else sql_database
        source = func(
            defer_table_reflect=defer_table_reflect,
            credentials=base["creds"],
            table_names=base["tables"],
            schema=schema,
            backend=config.backend,
            detect_precision_hints=True,
            backend_kwargs=config.backend_kwargs,
            query_adapter_callback=query_callback,
        ).parallelize()

    for table, columns in config.select_columns.items():
        if table in source.resources:
            source.resources[table].add_map(lambda doc, cols=columns: select_columns_f(doc, cols))

    if config.add_timestamp:
        for table in source.resources:
            source.resources[table].add_map(lambda row: {**row, "_updated_at": datetime.now()})

    for table, hint in config.hints.items():
        if table in source.resources:
            source.resources[table].apply_hints(**hint)

    if config.limit:
        source = source.add_limit(config.limit)

    if dest_driver == "snowflake":
        db_encoded = "__" in schema
        for resource in source.resources.values():
            if db_encoded:
                resource.apply_hints(table_name=f"{schema}__{resource.name}")
            else:
                resource.apply_hints(table_name=f"{database.replace('.', '_')}__{schema}__{resource.name}")

    if base["creds"].drivername != "filesystem":
        for resource in source.resources.values():
            resource_hints = resource.validator.resource_hints if resource.validator else {}
            has_pk = bool(resource_hints.get("primary_key"))
            is_inc = hasattr(resource, "incremental") and resource.incremental and getattr(resource.incremental, "cursor_path", None)
            disp = resource_hints.get("write_disposition") or resource.write_disposition

            if not has_pk and not is_inc and disp == "append":
                resource.apply_hints(write_disposition="replace")

    source_keys = base.get("source", {})

    translator = CustomDagsterDltTranslator(
        source_keys=source_keys,
        dest_database=dest_database,
        dest_schema=dest_schema,
        src_database=database,
        src_schema=schema,
        table_name=config.pipeline_kwargs.get("table_name"),
        src_platform=base["creds"].drivername,
        dest_driver=dest_driver,
        kinds=kinds,
        destination=dlt_pipeline.destination,
        source_creds=base["creds"],
    )

    @dlt_assets_with_io_managers(
        dlt_source=source.parallelize(),
        dlt_pipeline=dlt_pipeline,
        name=f"dlt_{config.name}_{schema}_asset",
        io_manager_key=config.io_manager_key,
        dagster_dlt_translator=translator,
    )
    def dlt_asset(context: AssetExecutionContext, dlt: DagsterDltResource, exec_config: DltAssetConfig):
        yield from dlt.run(context=context, **default_pipeline_kwargs, **exec_config.pipeline_kwargs)

    return dlt_asset


def create_dlt_assets(
    sources: List[Union[AssetKey, str]],
    source_config: Dict[str, Any],
    dest_config: Dict[str, Any],
    config: DltAssetGroupConfig,
    query_callback: Optional[Callable] = None,
    staging: Optional[Any] = None,
    staging_config: Optional[Dict[str, Any]] = None,
) -> List[Union[AssetsDefinition, AssetSpec]]:
    """Builds Dagster DLT multi-assets cleanly utilizing the Pydantic DltAssetGroupConfig component pattern.
    
    Args:
        sources: A list of AssetKeys or strings indicating tables to extract.
        source_config: The resource dictionary detailing the source DB properties.
        dest_config: The resource dictionary detailing the destination system properties.
        config: The unified `DltAssetGroupConfig` containing hints, names, limit, etc.
        query_callback: Optional callback to intercept raw db queries.
        staging: Optional staging object mapping mapping.
        staging_config: Config for intermediate staging areas.
    """
    
    mapping: Dict[str, Any] = {}
    _assets: List[Union[AssetsDefinition, AssetSpec]] = []
    staging_config = staging_config or {}

    default_pipeline_kwargs = {"loader_file_format": "parquet"} if dest_config.get("drivername") in ["filesystem", "databricks"] else {}
    default_pipeline_kwargs.update(config.pipeline_kwargs)

    if sources and isinstance(sources[0], AssetKey):
        process_sources(sources, mapping, source_config)
    else:
        kinds: List[str] = []
        creds = config_to_credentials(source_config, kinds)
        for table in sources:
            _assets.append(AssetSpec(key=AssetKey([source_config["database"].replace(".", "_"), source_config["schema"].replace(".", "_"), table]), kinds=kinds))
        mapping[source_config["database"]] = {
            source_config["schema"]: {"tables": sources, "creds": creds, "source_config": source_config}
        }

    kinds = []
    credentials = config_to_credentials(dest_config, kinds)
    
    if staging and staging_config:
        staging = get_destination(config_to_credentials(staging_config), staging_config.get("destination", {}), vars=ENV_VARS)

    for database, schema_tables in mapping.items():
        for schema, base in schema_tables.items():
            if hasattr(credentials, "database") and (not credentials.database or credentials.database == "default"):
                credentials.database = getattr(base["creds"], "database", database)
            
            destination = get_destination(credentials, dest_config.get("destination", {}), vars=ENV_VARS, database=database)
            effective_dest_schema = config.dest_schema or getattr(credentials, "schema", None) or getattr(base["creds"], "schema", None) or schema
            effective_name = config.name or (f"{credentials.database.replace('.', '_')}_{effective_dest_schema}" if getattr(credentials, "database", None) else database)

            dlt_pipeline = pipeline(
                pipeline_name=f"{effective_name}_pipeline",
                dataset_name=effective_dest_schema,
                destination=destination,
                staging=staging,
                progress="log",
                export_schema_path="schemas/export",
            )
            
            try:
                asset = instantiate_assets(
                    base, database, schema, query_callback, dlt_pipeline, 
                    getattr(credentials, "database", database), effective_dest_schema,
                    credentials.drivername, kinds, config, default_pipeline_kwargs
                )
                _assets.append(asset)
            except Exception as e:
                print(f"DLT {effective_name} assets could not be instantiated for {database}.{schema}: {e}")
                
    write_env_vars()
    return _assets

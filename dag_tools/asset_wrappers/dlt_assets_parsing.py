import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Union

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
    add_timestamp_f,
    config_to_credentials,
    make_add_timestamp,
    make_select_columns,
    select_columns_f,
    using_dagster_dev,
    write_env_vars,
    DltAssetConfig,
)
from dag_tools.asset_wrappers.sources.sql_ct_database import sql_ct_database
from dlt.sources.sql_database import sql_database
from dlt.sources.filesystem import filesystem, read_parquet
from dag_tools.utils.credentials import get_credentials
from dag_tools.utils.env import update_from_env


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
                config["bucket_url"] = f"{config['bucket_url']}/{database}"

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


def build_dlt_source(
    base: Dict[str, Any],
    database: str,
    schema: str,
    query_callback: Optional[Callable],
    config: DltAssetGroupConfig,
    dest_driver: str,
    defer_table_reflect: bool = True,
):
    """Build the dlt source for one (database, schema), WITHOUT Dagster.

    Lifted verbatim out of ``instantiate_assets``, which used to construct
    the source and wrap it in a ``@multi_asset`` in one breath. Nothing
    outside Dagster could therefore build a source, which is why iterating
    on hints, cursors or column selection meant a full definitions load --
    90-180 seconds for a location carrying Dagster, dbt, dlt and datahub --
    and required every OTHER component in that location to import cleanly
    first.

    Everything here is pure dlt: reflection, hints, maps, limits and the
    write-disposition normalisation. The Dagster translator and decorator
    stay in ``instantiate_assets``, which now calls this.
    """
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
            # write_disposition / table_name are not valid kwargs for the dlt
            # source constructor (dlt >= 1.23 dropped write_disposition from
            # sql_database()); they are applied via apply_hints / the translator.
            **{k: v for k, v in config.pipeline_kwargs.items()
               if k not in ("write_disposition", "table_name")}
        ).parallelize()

        write_disposition = config.pipeline_kwargs.get("write_disposition")
        if write_disposition:
            for resource in source.resources.values():
                resource.apply_hints(write_disposition=write_disposition)

    for table, columns in config.select_columns.items():
        if table in source.resources:
            source.resources[table].add_map(make_select_columns(columns))

    if config.add_timestamp:
        for table in source.resources:
            source.resources[table].add_map(make_add_timestamp())

    for table, hint in config.hints.items():
        if table in source.resources:
            source.resources[table].apply_hints(**hint)

    if config.limit:
        # count_rows=True, because the field is documented as a ROW limit
        # and dlt's default is not one. From dlt's own docstring: "dlt
        # counts number of 'yields/batches/pages' not the number of rows
        # inside" and "Empty pages/yields are also counted."
        #
        # With the sqlalchemy backend a resource yields PAGES, and the
        # first yield is not data, so the default made `limit: 1` extract
        # NOTHING -- no rows, no table -- while `limit: 2` extracted an
        # entire ten-row table. A knob whose "1" means zero and whose "2"
        # means everything is worse than no knob: it reads as working.
        source = source.add_limit(config.limit, count_rows=True)

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

    return source


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
    
    source = build_dlt_source(
        base, database, schema, query_callback, config, dest_driver,
        defer_table_reflect,
    )

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
        op_tags=config.effective_op_tags() or None,
        pool=config.pool,
    )
    def dlt_asset(context: AssetExecutionContext, dlt: DagsterDltResource, config: DltAssetConfig):
        yield from dlt.run(context=context, **default_pipeline_kwargs, **config.pipeline_kwargs)

    return dlt_asset


@dataclass
class DltRunnable:
    """One (source, pipeline) pair, ready to run outside Dagster.

    The dbt asymmetry this closes: a dbt project directory IS the artifact
    and dbt ships its own runner, so Dagster only ever wraps something
    already runnable. A dlt pipeline here is YAML that only this factory
    knows how to interpret, so nothing outside Dagster could construct one
    -- and iterating on a cursor or a hint meant a full definitions load.
    """

    source: Any
    pipeline: Any
    database: str
    schema: str
    tables: List[str]
    dest_schema: str

    def run(self, **kwargs):
        """Extract, normalize and load. Same call the Dagster asset makes."""
        return self.pipeline.run(self.source.parallelize(), **kwargs)

    def extract(self, **kwargs):
        """Extract ONLY -- no normalize, no load.

        Validates reflection, hints, cursors and the query adapter against
        the real source without writing anywhere, which is most of what a
        mapping change needs to prove.
        """
        return self.pipeline.extract(self.source.parallelize(), **kwargs)


def _prepare_units(
    sources,
    source_config: Dict[str, Any],
    dest_config: Dict[str, Any],
    config: DltAssetGroupConfig,
    staging: Optional[Any] = None,
    staging_config: Optional[Dict[str, Any]] = None,
    destination_override: Optional[Any] = None,
):
    """Resolve config into everything needed to build a source or an asset.

    Shared by ``create_dlt_assets`` and ``build_dlt_runnables`` so the two
    cannot drift: a local run that resolved credentials or named the
    pipeline differently from the deployed asset would be a dev loop that
    tests something other than what ships.

    ``destination_override`` replaces the configured destination -- the
    lever for pointing a real source at a local DuckDB file. It changes
    only where data LANDS; reflection, hints and cursors stay exactly as
    configured, which is what makes the local run meaningful.
    """
    dest_config = update_from_env(dest_config, True)
    source_config = update_from_env(source_config, True)
    staging_config = update_from_env(staging_config, True) or {}

    mapping: Dict[str, Any] = {}
    specs: List[Any] = []

    default_pipeline_kwargs = (
        {"loader_file_format": "parquet"}
        if dest_config.get("drivername") in ["filesystem", "databricks"]
        else {}
    )
    default_pipeline_kwargs.update(config.pipeline_kwargs)

    if sources and isinstance(sources[0], AssetKey):
        process_sources(sources, mapping, source_config)
    else:
        src_kinds: List[str] = []
        creds = config_to_credentials(source_config, src_kinds)
        for table in sources:
            specs.append(AssetSpec(
                key=AssetKey([
                    source_config["database"].replace(".", "_"),
                    source_config["schema"].replace(".", "_"),
                    table,
                ]),
                kinds=src_kinds,
            ))
        mapping[source_config["database"]] = {
            source_config["schema"]: {
                "tables": sources, "creds": creds, "source_config": source_config,
            }
        }

    kinds: List[str] = []
    credentials = config_to_credentials(dest_config, kinds)

    if staging and staging_config:
        staging = get_destination(
            config_to_credentials(staging_config),
            staging_config.get("destination", {}), vars=ENV_VARS,
        )

    units = []
    for database, schema_tables in mapping.items():
        for schema, base in schema_tables.items():
            if hasattr(credentials, "database") and (
                not credentials.database or credentials.database == "default"
            ):
                credentials.database = getattr(base["creds"], "database", database)

            destination = destination_override or get_destination(
                credentials, dest_config.get("destination", {}),
                vars=ENV_VARS, database=database,
            )
            effective_dest_schema = (
                config.dest_schema
                or getattr(credentials, "schema", None)
                or getattr(base["creds"], "schema", None)
                or schema
            )
            effective_name = config.name or (
                f"{credentials.database.replace('.', '_')}_{effective_dest_schema}"
                if getattr(credentials, "database", None) else database
            )

            dlt_pipeline = dlt.pipeline(
                pipeline_name=f"{effective_name}_pipeline",
                dataset_name=effective_dest_schema,
                destination=destination,
                staging=staging,
                progress="log",
                export_schema_path="schemas/export",
            )

            units.append({
                "base": base,
                "database": database,
                "schema": schema,
                "pipeline": dlt_pipeline,
                "dest_database": getattr(credentials, "database", database),
                "dest_schema": effective_dest_schema,
                "dest_driver": credentials.drivername,
                "kinds": kinds,
                "default_pipeline_kwargs": default_pipeline_kwargs,
            })

    return specs, units


def build_dlt_runnables(
    sources,
    source_config: Dict[str, Any],
    dest_config: Dict[str, Any],
    config: DltAssetGroupConfig,
    query_callback: Optional[Callable] = None,
    staging: Optional[Any] = None,
    staging_config: Optional[Dict[str, Any]] = None,
    destination_override: Optional[Any] = None,
    defer_table_reflect: bool = True,
) -> List[DltRunnable]:
    """The same pipelines the Dagster assets wrap, without Dagster.

    Built from the SAME resolution the asset path uses, so a local run
    exercises the configuration that ships rather than a parallel reading
    of it.
    """
    _specs, units = _prepare_units(
        sources, source_config, dest_config, config,
        staging=staging, staging_config=staging_config,
        destination_override=destination_override,
    )
    runnables = []
    for unit in units:
        source = build_dlt_source(
            unit["base"], unit["database"], unit["schema"], query_callback,
            config, unit["dest_driver"], defer_table_reflect,
        )
        runnables.append(DltRunnable(
            source=source,
            pipeline=unit["pipeline"],
            database=unit["database"],
            schema=unit["schema"],
            tables=list(unit["base"]["tables"]),
            dest_schema=unit["dest_schema"],
        ))
    write_env_vars()
    return runnables


def create_dlt_assets(
    sources: List[Union[AssetKey, str]],
    source_config: Dict[str, Any],
    dest_config: Dict[str, Any],
    config: DltAssetGroupConfig,
    query_callback: Optional[Callable] = None,
    staging: Optional[Any] = None,
    staging_config: Optional[Dict[str, Any]] = None,
) -> List[Union[AssetsDefinition, AssetSpec]]:
    """Builds Dagster DLT multi-assets from the unified DltAssetGroupConfig.

    Shares its config resolution with ``build_dlt_runnables`` via
    ``_prepare_units``, so the local dev loop and the deployed asset
    cannot resolve credentials, pipeline names or destinations differently.
    """
    specs, units = _prepare_units(
        sources, source_config, dest_config, config,
        staging=staging, staging_config=staging_config,
    )
    _assets: List[Union[AssetsDefinition, AssetSpec]] = list(specs)

    for unit in units:
        try:
            asset = instantiate_assets(
                unit["base"], unit["database"], unit["schema"], query_callback,
                unit["pipeline"], unit["dest_database"], unit["dest_schema"],
                unit["dest_driver"], unit["kinds"], config,
                unit["default_pipeline_kwargs"],
            )
            _assets.append(asset)
        except Exception as e:
            print(
                f"DLT {config.name} assets could not be instantiated for "
                f"{unit['database']}.{unit['schema']}: {e}"
            )

    write_env_vars()
    return _assets

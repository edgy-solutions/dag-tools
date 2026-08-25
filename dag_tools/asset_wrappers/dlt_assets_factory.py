import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Union

import dlt
from dlt.common.configuration.specs.azure_credentials import AzureCredentials
from dlt.common.storages.configuration import AwsCredentials, FileSystemCredentials
from dlt.destinations.impl.clickhouse.configuration import ClickHouseCredentials
from dlt.destinations.impl.databricks.configuration import DatabricksCredentials
from dlt.sources.credentials import ConnectionStringCredentials
from pydantic import Field as PydanticField, BaseModel

from dagster import (
    AssetKey,
    AssetSpec,
    AssetsDefinition,
    AutoMaterializePolicy,
    BackfillPolicy,
    Config,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_dlt.translator import DagsterDltTranslator, DltResourceTranslatorData
from dag_tools.components.datahub_lineage import get_datahub_metadata

logger = logging.getLogger("dlt_manager")

ENV_VARS: Dict[str, Any] = {}
KIND_MAPPING: Dict[str, str] = {"mssql": "sqlserver"}
TYPE_MAPPINGS: Dict[str, str] = {"postgres": "postgresql"}


class DLTAssetSchedule(BaseModel):
    """Defines a schedule for a specific set of DLT assets."""
    name: str
    keys: List[AssetKey]
    schedule: str
    timezone: str = "America/New_York"


class DltAssetGroupConfig(BaseModel):
    """Configuration class for generating a DLT Asset group pipeline cleanly.
    
    This replaces the massive kwargs dictionary/arguments historically passed 
    to create_dlt_assets, natively aligning with Dagster's new custom/declarative 
    component structures.
    """
    name: Optional[str] = PydanticField(
        default=None, description="The custom name for the DLT pipeline/asset group."
    )
    dest_schema: Optional[str] = PydanticField(
        default=None, description="Override the target destination schema."
    )
    backend: str = PydanticField(
        default="sqlalchemy", description="The backend processing engine (e.g., sqlalchemy, pyarrow)."
    )
    backend_kwargs: Dict[str, Any] = PydanticField(
        default_factory=dict, description="Arguments to pass to the backend processor."
    )
    pipeline_kwargs: Dict[str, Any] = PydanticField(
        default_factory=dict, description="Pipeline-level arguments (e.g., write_disposition, table_name)."
    )
    hints: Dict[str, Any] = PydanticField(
        default_factory=dict, description="Source table hints (e.g., incremental cursors, primary keys)."
    )
    select_columns: Dict[str, List[str]] = PydanticField(
        default_factory=dict, description="Map of table names to columns to retain."
    )
    limit: int = PydanticField(
        default=0, description="Row limit for non-production runs."
    )
    add_timestamp: bool = PydanticField(
        default=False, description="Whether to append _updated_at columns to extracted rows."
    )
    io_manager_key: str = PydanticField(
        default="io_manager", description="The Dagster IO manager key to assign to the multi_asset."
    )
    op_tags: Dict[str, Any] = PydanticField(
        default_factory=dict,
        description=(
            "Tags forwarded verbatim to the generated `@multi_asset`'s `op_tags`. "
            "The primary use is per-pipeline k8s resource requests / limits / "
            "node selectors / tolerations via the `dagster-k8s/config` key that "
            "the k8s executor + run launcher read. Build the dict manually or "
            "via `dag_tools.utils.k8s.resolve_k8s_resource_tags(<PREFIX>)` which "
            "returns the correct shape from env vars. Example:\n"
            "  op_tags:\n"
            "    dagster-k8s/config:\n"
            "      container_config:\n"
            "        resources:\n"
            "          requests: {cpu: '2000m', memory: '8Gi'}\n"
            "          limits:   {cpu: '4000m', memory: '16Gi'}"
        ),
    )
    pool: Optional[str] = PydanticField(
        default=None,
        description=(
            "Optional Dagster concurrency pool name for the generated "
            "`@multi_asset`. Runs sharing a pool respect the pool's slot "
            "limit configured on the instance."
        ),
    )
    k8s_resource_env_prefix: Optional[str] = PydanticField(
        default=None,
        description=(
            "Env-prefix convention for per-pipeline k8s resources, matching "
            "the deployment pattern used elsewhere (e.g. doc-tools' "
            "`resolve_k8s_resource_tags(prefix=...)` on `@asset`). When set, "
            "the component resolves `<PREFIX>_CPU_REQUEST`, `<PREFIX>_MEM_REQUEST`, "
            "`<PREFIX>_CPU_LIMIT`, `<PREFIX>_MEM_LIMIT` from the code-location's "
            "environment into the `dagster-k8s/config` op_tag at defs-load time. "
            "The deployment sets those four env vars (Helm `env:`), and the YAML "
            "just names the prefix — no need to template four values into "
            "`op_tags`. Any explicit `op_tags` are deep-merged ON TOP (so you "
            "can add node selectors / tolerations, or override an individual "
            "resource value). Limits default to requests when unset."
        ),
    )
    k8s_default_cpu: str = PydanticField(
        default="500m",
        description="Fallback CPU request when k8s_resource_env_prefix is set but "
                    "<PREFIX>_CPU_REQUEST is unset in the environment.",
    )
    k8s_default_mem: str = PydanticField(
        default="1Gi",
        description="Fallback memory request when k8s_resource_env_prefix is set but "
                    "<PREFIX>_MEM_REQUEST is unset in the environment.",
    )

    def effective_op_tags(self) -> Dict[str, Any]:
        """Op-tags actually applied to the generated ``@multi_asset``.

        Resolves ``k8s_resource_env_prefix`` (if set) into the
        ``dagster-k8s/config`` resource shape via
        :func:`dag_tools.utils.k8s.resolve_k8s_resource_tags`, then
        deep-merges any explicit :attr:`op_tags` on top — explicit values
        win on leaf conflicts, so an operator can override a single
        resource value or add unrelated tags (node selectors, tolerations)
        alongside the env-driven resources.
        """
        from dag_tools.utils.k8s import resolve_op_tags_with_env_prefix

        return resolve_op_tags_with_env_prefix(
            env_prefix=self.k8s_resource_env_prefix,
            explicit_tags=self.op_tags,
            default_cpu=self.k8s_default_cpu,
            default_mem=self.k8s_default_mem,
        )


def include_actual_dlt_assets(
    asset_keys: List[AssetKey], dlt_assets: List[Union[AssetSpec, AssetsDefinition]]
) -> List[AssetKey]:
    """Filters a list of asset keys to only include those present in the provided DLT assets."""
    valid_keys = {
        key
        for dlt_asset in dlt_assets
        if not isinstance(dlt_asset, AssetSpec)
        for key in dlt_asset.keys
    }
    return [
        asset for asset in asset_keys if asset.path[0] != "dlt" or asset in valid_keys
    ]


def add_dlt_schedule(
    dlt_assets: List[Union[AssetSpec, AssetsDefinition]],
    schedules: List[ScheduleDefinition],
    schedule: DLTAssetSchedule,
) -> None:
    selection = include_actual_dlt_assets(schedule.keys, dlt_assets)
    if selection:
        job = define_asset_job(schedule.name, selection=selection)
        schedules.append(
            ScheduleDefinition(
                job=job,
                cron_schedule=schedule.schedule,
                execution_timezone=schedule.timezone,
            )
        )


def add_dlt_schedules(
    dlt_assets: List[Union[AssetSpec, AssetsDefinition]],
    schedules: List[ScheduleDefinition],
    dlt_schedule_list: List[DLTAssetSchedule],
) -> None:
    for schedule in dlt_schedule_list:
        add_dlt_schedule(dlt_assets, schedules, schedule)


def config_to_credentials(
    config: Dict[str, Any], kinds: Optional[List[str]] = None
) -> Any:
    """Converts a standardized configuration dictionary into a DLT Credential object."""
    if kinds is None:
        kinds = []

    drivername = config.get("drivername") or config.get("protocol") or config.get("resource", "")

    # If not explicitly set, try to infer drivername from a 'credentials' string
    if not drivername and config.get("credentials") and isinstance(config.get("credentials"), str):
        try:
            drivername = config["credentials"].split("://")[0]
        except Exception:
            pass

    kind = drivername.split("+")[0]

    if drivername == "snowflake":
        creds = SnowflakeCredentials()
    elif drivername == "filesystem":
        if "aws_access_key_id" in config:
            kind = "minio"
            creds = AwsCredentials()
        elif "azure_storage_account_name" in config:
            kind = "azure"
            creds = AzureCredentials()
        else:
            creds = FileSystemCredentials()
    elif drivername == "clickhouse":
        creds = ClickHouseCredentials()
    elif drivername == "databricks":
        if "azure_storage_account_name" in config:
            drivername = "filesystem"
            kinds.append("azure")
            creds = AzureCredentials()
        else:
            creds = DatabricksCredentials()
    else:
        creds = ConnectionStringCredentials()

    creds.drivername = TYPE_MAPPINGS.get(drivername, drivername)
    kinds.append(KIND_MAPPING.get(kind, kind))

    # If we have a raw connection string provided, parse it into the credentials object
    if isinstance(creds, ConnectionStringCredentials) and config.get("credentials") and isinstance(config.get("credentials"), str):
        try:
            creds.parse_native_representation(config["credentials"])
        except Exception:
            pass

    STANDARD_ATTRS = {"host", "port", "username", "password", "database", "drivername", "schema"}
    if not hasattr(creds, "query") or creds.query is None:
        creds.query = {}
    
    is_connection_string = isinstance(creds, ConnectionStringCredentials)
    
    for key, item in config.items():
        if key == "credentials":
            continue

        if key in STANDARD_ATTRS:
            # If we already have a full DSN, avoid setting individual DB/Schema attrs 
            # which can cause DLT/SQLAlchemy parsing conflicts
            if is_connection_string and key in ["database", "schema"]:
                continue
            setattr(creds, key, item)
        elif key not in ["destination", "drivername", "type"]:
             creds.query[key] = item

    return creds


_pa: Any = None  # the pyarrow module, or False once we know it is unavailable


def _arrow_item(item: Any) -> bool:
    """True for the chunk types the arrow-shaped backends yield.

    `add_map` fires once per item a resource yields, and the item's shape
    depends on the backend: `sqlalchemy` yields dicts (one row), `pyarrow` /
    `connectorx` and the filesystem + `read_parquet` path yield a whole chunk
    as a table or a record batch. Import lazily and remember the answer —
    this is on the per-item path of every extract.
    """
    global _pa
    if _pa is None:
        try:
            import pyarrow  # noqa: PLC0415

            _pa = pyarrow
        except ImportError:
            _pa = False
    return _pa is not False and isinstance(item, (_pa.Table, _pa.RecordBatch))


def select_columns_f(doc: Any, select_columns: Optional[List[str]] = None) -> Any:
    if not select_columns:
        return doc
    if _arrow_item(doc):
        return doc.select([c for c in select_columns if c in doc.schema.names])
    if isinstance(doc, list):
        return [select_columns_f(row, select_columns) for row in doc]
    return {k: doc[k] for k in select_columns if k in doc}


def add_timestamp_f(item: Any, column: str = "_updated_at") -> Any:
    """Stamp a load timestamp onto a dlt item, whatever shape the backend yields.

    Always UTC-aware and, on the arrow path, explicitly typed — a naive
    datetime infers a different destination column type than an aware one, so
    loading one table through two backends would otherwise drift the schema.
    Note the arrow branch stamps one timestamp per chunk rather than per row.
    """
    ts = datetime.now(timezone.utc)

    if _arrow_item(item):
        stamp = _pa.array([ts] * item.num_rows, type=_pa.timestamp("us", tz="UTC"))
        existing = item.schema.get_field_index(column)
        if existing >= 0:
            # Overwrite, matching the dict path — append_column would leave the
            # chunk with two same-named columns and fail in normalization.
            return item.set_column(existing, column, stamp)
        return item.append_column(column, stamp)

    if isinstance(item, dict):
        return {**item, column: ts}
    if isinstance(item, list):
        return [{**row, column: ts} for row in item]

    raise TypeError(
        f"add_timestamp cannot stamp a {type(item).__name__} item; "
        "supported shapes are dict, list of dicts, pyarrow.Table and pyarrow.RecordBatch"
    )


def make_add_timestamp(column: str = "_updated_at") -> Callable[[Any], Any]:
    """A ONE-ARGUMENT stamper, which is what ``add_map`` requires.

    dlt decides how to invoke a map function by COUNTING its parameters,
    not by reading their names::

        # dlt/extract/items_transform.py
        if len(sig.parameters) == 1:
            self._f = transform_f
        else:                       # TODO: do better check
            self._f_meta = transform_f

    Anything with two or more parameters is called as ``f(item, meta)``.
    So passing ``add_timestamp_f`` -- whose second parameter is ``column``
    -- straight to ``add_map`` binds ``column = meta``, and meta is
    normally None. Three different failures came out of that one line:

      * arrow items died on ``get_field_index(None)`` with
        ``TypeError: expected bytes, NoneType found``;
      * dict items silently grew a column literally named ``None``;
      * the same trap on ``select_columns`` made it a quiet no-op.

    The error dlt raises claims the second argument must be *named*
    ``meta``. It does not check that -- only the count -- so a
    plausible-looking signature passes inspection and misbehaves.
    """
    def _add_timestamp(item: Any) -> Any:
        return add_timestamp_f(item, column)

    return _add_timestamp


def make_select_columns(columns: Optional[List[str]]) -> Callable[[Any], Any]:
    """A ONE-ARGUMENT column filter. See :func:`make_add_timestamp`.

    The previous call site used ``lambda doc, cols=columns: ...``, whose
    default argument reads as safe binding but is a second parameter, so
    dlt overwrote it with meta. ``cols`` became None, ``select_columns_f``
    hit its ``if not select_columns: return doc`` guard, and every
    configured column selection silently did nothing.
    """
    def _select_columns(item: Any) -> Any:
        return select_columns_f(item, columns)

    return _select_columns


def db_supports_schema(platform: str) -> bool:
    return platform != "clickhouse"


class CustomDagsterDltTranslator(DagsterDltTranslator):
    """Custom translator for mapping DLT resources to normalized Dagster AssetKeys."""
    REMAP = {"postgresql": "postgres"}

    def __init__(
        self,
        source_keys: Dict[str, List[str]],
        dest_database: str,
        dest_schema: str,
        src_database: str,
        src_schema: str,
        materialize_policy: AutoMaterializePolicy = AutoMaterializePolicy.eager(),
        table_name: Optional[str] = None,
        src_platform: Optional[str] = None,
        dest_driver: Optional[str] = None,
        kinds: Optional[List[str]] = None,
        destination: Any = None,
        source_creds: Any = None,
    ) -> None:
        self.source_keys = source_keys
        self.dest_database = dest_database
        self.dest_schema = dest_schema
        self.src_database = src_database
        self.src_schema = src_schema
        self.policy = materialize_policy
        self.table_name = table_name
        self.kinds = kinds or []
        self.dest_driver = dest_driver

        if src_platform:
            base_platform = src_platform.split("+")[0]
            if base_platform == "filesystem":
                self.src_platform = self.get_platform_from_entity(source_creds)
            else:
                self.src_platform = self.REMAP.get(base_platform, base_platform)
        else:
            self.src_platform = None

        self.base_distinction = self.get_destination_distinctions(destination)

    @staticmethod
    def get_destination_distinctions(destination: Any) -> List[str]:
        if not destination or not hasattr(destination, "config_params"):
            return ["filesystem"]

        params = destination.config_params
        creds = params.get("credentials")

        if isinstance(creds, AwsCredentials) and "bucket_url" in params:
            bucket = params["bucket_url"].split(":")[1].split("/")[2]
            match = re.search(r"[^:]*://([^\.]*)\..*", creds.endpoint_url)
            return [match.group(1), bucket] if match else [bucket]

        elif isinstance(creds, AzureCredentials) and "bucket_url" in params:
            match = re.search(r"[^:]*://([^@]*)@([^/]*)/([^/]*)/.*", params["bucket_url"])
            return [match.group(2), match.group(1), match.group(3)] if match else [creds.azure_account_host]

        elif isinstance(creds, DatabricksCredentials):
            return [creds.server_hostname.split(".")[0]]

        elif isinstance(creds, ConnectionStringCredentials):
            return [creds.host.split(".")[0]]

        return ["filesystem"]

    @staticmethod
    def get_platform_from_entity(credentials: Any) -> str:
        if isinstance(credentials, AwsCredentials):
            return "s3"
        elif isinstance(credentials, AzureCredentials):
            return "abs"
        return "filesystem"

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        default_spec = super().get_asset_spec(data)
        resource_name = data.resource.name
        
        deps = self.source_keys.get(resource_name) or [
            self.src_database.replace(".", "_"),
            self.src_schema.replace(".", "_"),
            resource_name,
        ]

        key = ["dlt", self.dest_database]
        target_table = self.table_name or data.resource.table_name.lower()

        if db_supports_schema(self.dest_driver):
            key.extend([self.dest_schema, target_table])
        else:
            key.append(f"{self.dest_schema}___{target_table}")

        if key == deps or self.dest_driver == "filesystem":
            key = [key[0]] + self.base_distinction + key[1:]

        self.kinds.append("dlt")

        return default_spec.replace_attributes(
            key=AssetKey(key),
            deps=[AssetKey(deps)],
            kinds=self.kinds,
            metadata=get_datahub_metadata([deps], platform=self.src_platform) if self.src_platform else {},
            automation_condition=self.policy.to_automation_condition(),
        )


def using_dagster_dev() -> bool:
    return bool(os.getenv("DAGSTER_IS_DEV_CLI"))

def write_env_vars() -> None:
    if using_dagster_dev():
        with open(".env.dlt", "w") as f:
            for key, item in ENV_VARS.items():
                f.write(f"{key}={item}\n")


class DltAssetConfig(Config):
    """Execution-time configuration parameters passed to the IO manager."""
    pipeline_kwargs: Optional[Dict[str, str]] = PydanticField(default_factory=dict)

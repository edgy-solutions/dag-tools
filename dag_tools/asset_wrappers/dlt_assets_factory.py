import logging
import os
import re
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

    for key, item in config.items():
        if key not in ["destination", "drivername"]:
            setattr(creds, key, item)

    return creds


def select_columns_f(doc: Dict[str, Any], select_columns: Optional[List[str]] = None) -> Dict[str, Any]:
    if not select_columns:
        return doc
    return {k: doc[k] for k in select_columns if k in doc}


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

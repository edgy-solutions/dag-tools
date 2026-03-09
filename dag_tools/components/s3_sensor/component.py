import copy
from typing import Any, Dict, Optional, Sequence

import pyarrow as pa
from pydantic import Field

from dagster import (
    AssetSelection,
    Definitions,
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    asset,
    define_asset_job,
    sensor,
)
import dagster._check as check
from dagster.components import Component

from dag_tools.resources.arrow import ArrowResource
from dag_tools.resources.s3 import S3SensorConfig, S3SensorResource


MAX_KEYS = 1000


def get_s3_keys(bucket: str, s3_session: Any, log: Any, prefix: str = "", since_key: Optional[str] = None) -> Dict[str, Any]:
    """Retrieves chronological S3 keys using LastModified for cursor pagination."""
    check.str_param(bucket, "bucket")
    check.str_param(prefix, "prefix")
    check.opt_str_param(since_key, "since_key")

    cursor = ""
    contents = []

    while True:
        response = s3_session.list_objects_v2(
            Bucket=bucket,
            Delimiter="",
            MaxKeys=MAX_KEYS,
            Prefix=prefix,
            StartAfter=cursor,
        )
        contents.extend(response.get("Contents", []))
        if response.get("KeyCount", 0) < MAX_KEYS:
            break
        cursor = response["Contents"][-1]["Key"]

    # Deduplicate and sort by chronological time to ensure cursor stability
    sorted_keys = {
        str(obj["LastModified"]): obj 
        for obj in sorted(contents, key=lambda x: x.get("LastModified"))
    }

    if not since_key or since_key not in sorted_keys:
        return sorted_keys

    # Find the since_key and return only elements after it
    keys_list = list(sorted_keys.keys())
    for idx, key in enumerate(keys_list):
        if key == since_key:
            return {
                k: sorted_keys[k] 
                for k in keys_list[idx + 1 :]
            }
    return {}


def key_2_partition_key(key: str) -> str:
    """Default key extractor returning the raw S3 string path as the partition."""
    return key


def get_dynamic_partitions_requests(tables_partitions_def: Any, keys: Dict[str, Any], key_extract_func=key_2_partition_key):
    """Generates Dagster partition injection requests dynamically based on detected keys."""
    if isinstance(tables_partitions_def, MultiPartitionsDefinition):
        dimensions = {}
        items = [key_extract_func(key) for key in list(keys.values())]
        for item in items:
            for key, value in item.keys_by_dimension.items():
                if isinstance(tables_partitions_def.get_partitions_def_for_dimension(key), DynamicPartitionsDefinition):
                    dimensions.setdefault(key, set()).add(value)
        return [
            tables_partitions_def.get_partitions_def_for_dimension(dimension).build_add_request(list(keys_set)) 
            for dimension, keys_set in dimensions.items()
        ]
    return [tables_partitions_def.build_add_request(list(keys.values()))]


def update_file_url(ops: Dict[str, Any], config_dict: Dict[str, Any], url: str) -> Dict[str, Any]:
    """Injects the literal S3 URI into the operational config."""
    config_dict['file_url'] = url
    return copy.deepcopy(ops)


class S3ToArrowComponent(Component):
    """A declarative Dagster Component that monitors an S3 bucket and automatically maps 
    new files to PyArrow Datasets matching a Dynamic Partition schema.
    """
    
    partition_name: str = Field(description="The name of the dynamic partition definition (e.g., 'daily_logs').")
    bucket: str = Field(description="The highly-configurable S3 bucket to monitor.")
    prefix: Optional[str] = Field(default=None, description="The S3 Object Prefix filter.")
    io_manager_key: str = Field(default="io_manager", description="The IO Manager responsible for persisting the datasets.")
    delimiter: str = Field(default=",", description="CSV parsing delimiter if applicable.")
    
    def build_defs(self, context) -> Definitions:
        clean_name = self.partition_name.replace("-", "_")
        asset_name = f"{clean_name}_2arrow"
        sensor_name = f"{clean_name}_s3_sensor"
        job_name = f"ingest_s3_objects_{clean_name}"
        resource_key = f"s3_{clean_name}_sensor_resource"
        
        # 1. Define the dynamic partition mapping schema
        partitions_def = DynamicPartitionsDefinition(name=self.partition_name)
        
        # 2. Define the unified PyArrow Extraction Asset Component
        @asset(
            name=asset_name,
            compute_kind='pyarrow',
            io_manager_key=self.io_manager_key,
            config_schema={"file_url": str, "delimiter": Field(default=self.delimiter)},
            partitions_def=partitions_def,
        )
        def s3_arrow_asset(context, arrow_client: ArrowResource) -> Any:
            filename = context.op_config["file_url"]
            delimiter = context.op_config["delimiter"]
            partition = context.asset_partition_key_for_output()
            
            context.log.info(f"Ingesting partition {partition} from S3 URL: {filename}")
            # Ensure the pyarrow dataset abstracts S3 interactions globally
            return arrow_client.get_client().load_input_from_file(filename, context.log, delimiter)

        # 3. Create the targeted Dagster Job that the sensor executes
        sensor_job = define_asset_job(
            name=job_name,
            selection=AssetSelection.assets(s3_arrow_asset),
            partitions_def=partitions_def
        )
        
        # Determine base configurations for dynamic merging inside the loop
        base_op_config = {"ops": {asset_name: {"config": {"file_url": ""}}}}
        url_config_ref = base_op_config['ops'][asset_name]['config']

        # 4. Construct the globally responsive S3 Sensor
        @sensor(job=sensor_job, name=sensor_name, required_resource_keys={resource_key})
        def unified_s3_sensor(context: SensorEvaluationContext):
            # Resolve the S3 implementation via the injected dagster runtime resource mapping
            s3_resource = getattr(context.resources, resource_key)
            
            s3_prefix = self.prefix if self.prefix else s3_resource.config.s3_prefix
            s3_prefix = f"{s3_prefix}/" if s3_prefix and not s3_prefix.endswith("/") else (s3_prefix or "")
            
            since_key = context.cursor or None
            
            new_s3_keys = get_s3_keys(
                bucket=self.bucket,
                s3_session=s3_resource.get_client(),
                log=context.log,
                prefix=s3_prefix,
                since_key=since_key
            )

            if not new_s3_keys:
                return SkipReason(f"No new S3 objects found in s3://{self.bucket}/{s3_prefix}.")

            # Compute cursor safety dynamically, grabbing the very last LastModified ISO stamp string
            last_key = list(new_s3_keys.keys())[-1]
            
            # Filter and re-map key paths dynamically from ETag to true object paths
            filtered_keys = {
                f"{item['ETag']}-{item['Key']}": item["Key"].replace(s3_prefix, '', 1) 
                for item in new_s3_keys.values() 
                if s3_resource.apply_filter(item["Key"])
            }
            
            run_requests = [
                RunRequest(
                    run_key=etag,
                    partition_key=key_2_partition_key(s3_key),
                    run_config=update_file_url(
                        base_op_config, 
                        url_config_ref, 
                        f"s3://{self.bucket}/{s3_prefix}{s3_key}"
                    )
                )
                for etag, s3_key in filtered_keys.items()
            ]
            
            context.update_cursor(last_key)
            
            return SensorResult(
                run_requests=run_requests, 
                dynamic_partitions_requests=get_dynamic_partitions_requests(partitions_def, filtered_keys)
            )

        # 5. Bind and aggregate definitions globally into the Component Context
        return Definitions(
            assets=[s3_arrow_asset],
            sensors=[unified_s3_sensor],
            jobs=[sensor_job],
            resources={
                # Providing hardcoded defaults directly integrated with Component configurations
                resource_key: S3SensorResource(config=S3SensorConfig(s3_bucket=self.bucket, s3_prefix=self.prefix)),
                "arrow_client": ArrowResource()
            }
        )

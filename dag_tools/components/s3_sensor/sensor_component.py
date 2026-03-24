from typing import Any, Dict, List, Optional, Annotated

from dagster import (
    Definitions,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
    DefaultSensorStatus,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.model import Model, Resolver
from pydantic import Field

from dag_tools.resources.s3 import S3SensorConfig, S3SensorResource
from .utils import get_s3_keys, key_2_partition_key, get_dynamic_partitions_requests

class S3SensorComponent(Component, Model):
    """A standalone S3 Sensor Component that monitors a bucket and triggers a target job
    with file-level RunRequests and Dynamic Partition registration.
    """
    
    bucket: str = Field(description="The S3 bucket to monitor.")
    prefix: str = Field(description="The S3 prefix (directory) to monitor.")
    partition_name: str = Field(description="The name of the dynamic partition definition for tracking file state.")
    target_job: str = Field(description="The name of the Dagster job to trigger.")
    target_op: str = Field(description="The name of the op within the job that accepts the 'file_url' configuration.")
    
    filter_patterns: Annotated[
        List[str],
        Resolver.default(description="List of string substrings to exclude from triggering (e.g., metadata.json).")
    ] = ["/generated/", "metadata.json"]
    
    default_status: Annotated[
        str,
        Resolver.default(description="Default status of the sensor (RUNNING or STOPPED).")
    ] = "STOPPED"

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        clean_prefix = self.prefix.strip("/").replace("/", "_")
        sensor_name = f"{clean_prefix}_s3_sensor"
        resource_key = f"s3_{clean_prefix}_sensor_resource"
        
        status = DefaultSensorStatus.RUNNING if self.default_status == "RUNNING" else DefaultSensorStatus.STOPPED

        @sensor(
            name=sensor_name,
            job_name=self.target_job,
            required_resource_keys={resource_key},
            default_status=status,
        )
        def s3_managed_sensor(sensor_context: SensorEvaluationContext):
            s3_resource = getattr(sensor_context.resources, resource_key)
            s3_prefix = self.prefix
            s3_prefix = f"{s3_prefix}/" if s3_prefix and not s3_prefix.endswith("/") else (s3_prefix or "")
            
            since_key = sensor_context.cursor or None
            
            # Use the paginated utility to fetch ALL relevant keys
            all_s3_keys = get_s3_keys(
                bucket=self.bucket,
                s3_session=s3_resource.get_client(),
                log=sensor_context.log,
                prefix=s3_prefix,
                since_key=since_key
            )

            if not all_s3_keys:
                return SkipReason(f"No new objects found in s3://{self.bucket}/{s3_prefix} after cursor {since_key}.")

            # Compute cursor safety dynamically
            last_key = list(all_s3_keys.keys())[-1]
            
            # Apply filters (e.g. skip metadata.json)
            filtered_keys = {}
            for etag_key, item in all_s3_keys.items():
                obj_key = item["Key"]
                if any(pattern in obj_key for pattern in self.filter_patterns):
                    continue
                if not s3_resource.apply_filter(obj_key):
                    continue
                
                # We use the ETag + Key as a unique run_key
                # But the partition_key is the relative path
                rel_path = obj_key.replace(s3_prefix, '', 1)
                filtered_keys[f"{item.get('ETag', 'no_tag')}-{obj_key}"] = rel_path

            if not filtered_keys:
                sensor_context.update_cursor(last_key)
                return SkipReason(f"All {len(all_s3_keys)} new objects filtered out by patterns.")

            run_requests = [
                RunRequest(
                    run_key=run_key,
                    partition_key=rel_path,
                    run_config={
                        "ops": {
                            self.target_op: {
                                "config": {
                                    "file_url": f"s3://{self.bucket}/{s3_prefix}{rel_path}"
                                }
                            }
                        }
                    }
                )
                for run_key, rel_path in filtered_keys.items()
            ]
            
            sensor_context.update_cursor(last_key)
            
            # We need to register the partitions to the instance
            # Since S3SensorComponent doesn't own the partition definition, we just use the name
            # Dagster 1.12 handles dynamic partition addition via the SensorResult or instance
            return SensorResult(
                run_requests=run_requests,
                # Use a dummy dict for get_dynamic_partitions_requests or similar if we can't access the def
                # Actually, we can just return the raw partition keys in a request
                dynamic_partitions_requests=[
                    sensor_context.instance.add_dynamic_partitions(self.partition_name, list(filtered_keys.values()))
                ] if hasattr(sensor_context.instance, 'add_dynamic_partitions') else []
            )

        return Definitions(
            sensors=[s3_managed_sensor],
            resources={
                resource_key: S3SensorResource(config=S3SensorConfig(s3_bucket=self.bucket, s3_prefix=self.prefix))
            }
        )

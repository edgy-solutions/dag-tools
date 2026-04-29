from typing import Any, Dict, List, Optional, Annotated

from dagster import (
    Definitions,
    AddDynamicPartitionsRequest,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
    DefaultSensorStatus,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model, Resolver
from dagster_aws.s3 import S3Resource
from pydantic import Field

from dag_tools.resources.s3 import S3ResourceConfig, S3SensorResource
from .utils import get_s3_keys


class S3SensorComponent(Component, Resolvable, Model):
    """A standalone S3 Sensor Component that monitors a bucket and triggers a target job
    with file-level RunRequests and Dynamic Partition registration.
    """
    
    bucket: str = Field(description="The S3 bucket to monitor.")
    prefix: str = Field(description="The S3 prefix (directory) to monitor.")
    name: Optional[str] = Field(default=None, description="Optional custom name for the sensor. Overrides the prefix-based naming fallback.")
    partition_name: str = Field(description="The name of the dynamic partition definition for tracking file state.")
    target_job: str = Field(description="The name of the Dagster job to trigger.")
    target_op: str = Field(description="The name of the op within the job that accepts the 'file_url' configuration.")
    
    s3_resource: Annotated[
        Optional[Dict[str, Any]], 
        Resolver.default(description="Optional custom configuration for the underlying S3Resource (e.g. endpoint_url).")
    ] = None
    
    s3_filter: Annotated[
        Optional[str],
        Resolver.default(description="Optional regex filter to match S3 keys.")
    ] = None

    filter_patterns: Annotated[
        List[str],
        Resolver.default(description="List of string substrings to exclude from triggering (e.g., metadata.json).")
    ] = ["/generated/", "metadata.json"]
    
    default_status: Annotated[
        str,
        Resolver.default(description="Default status of the sensor (RUNNING or STOPPED).")
    ] = "STOPPED"

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        clean_prefix = self.prefix.strip("/").replace("/", "_").replace("-", "_")
        clean_name = self.name.replace("-", "_") if self.name else None
        sensor_name = clean_name or (f"{clean_prefix}_s3_sensor" if clean_prefix else "s3_sensor")
        resource_key = f"{sensor_name}_resource"
        
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
            
            # Apply filters (e.g. skip metadata.json and custom regex)
            filtered_keys = {}
            for item in all_s3_keys.values():
                obj_key = item["Key"]
                if any(pattern in obj_key for pattern in self.filter_patterns):
                    continue
                if not s3_resource.apply_filter(obj_key):
                    continue
                
                # We use the ETag + Key as a unique run_key
                # The partition_key must not contain slashes to avoid breaking Dagster UI routing
                # We replace slashes with double underscores (__) to keep the full path visible, unique, and URL-safe
                partition_key = obj_key.replace("/", "__")
                filtered_keys[f"{item.get('ETag', 'no_tag')}-{obj_key}"] = (partition_key, obj_key)

            if not filtered_keys:
                sensor_context.update_cursor(last_key)
                return SkipReason(f"All {len(all_s3_keys)} new objects filtered out by patterns.")

            run_requests = [
                RunRequest(
                    run_key=run_key,
                    partition_key=partition_key,
                    run_config={
                        "ops": {
                            self.target_op: {
                                "config": {
                                    "file_url": f"s3://{self.bucket}/{obj_key}"
                                }
                            }
                        }
                    }
                )
                for run_key, (partition_key, obj_key) in filtered_keys.items()
            ]
            
            sensor_context.update_cursor(last_key)
            
            # We need to register the partitions to the instance
            # Since S3SensorComponent doesn't own the partition definition, we just use the name
            # Dagster 1.12 handles dynamic partition addition via the SensorResult or instance
            return SensorResult(
                run_requests=run_requests,
                dynamic_partitions_requests=[
                    AddDynamicPartitionsRequest(
                        partitions_def_name=self.partition_name,
                        partition_keys=[pk for pk, _ in filtered_keys.values()]
                    )
                ] if filtered_keys else []
            )

        # Initialize the underlying S3Resource
        # If s3_resource is a dict, we unpack it; otherwise we use default constructor
        underlying_s3 = S3Resource(**(self.s3_resource or {}))

        return Definitions(
            sensors=[s3_managed_sensor],
            resources={
                resource_key: S3SensorResource(
                    config=S3ResourceConfig(
                        s3_bucket=self.bucket,
                        s3_prefix=self.prefix,
                        s3_resource=underlying_s3,
                        s3_filter=self.s3_filter
                    )
                )
            }
        )

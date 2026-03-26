import re
from typing import Any, Dict, Optional

from dagster import ConfigurableResource
from dagster_aws.s3 import S3Resource
from pydantic import Field as PydanticField

from dag_tools.utils.helper import ConfigureFromDict


class S3ResourceConfig(ConfigurableResource):
    """Configuration schema for S3-based resources and sensors.
    
    This encapsulates the bucket, prefix, underlying S3 credentials (via S3Resource),
    and an optional regex filter for key matching.
    """

    s3_bucket: str = PydanticField(
        description="The AWS S3 bucket name to scan for incoming files."
    )
    s3_prefix: str = PydanticField(
        default="", 
        description="The S3 prefix folder path."
    )
    s3_resource: S3Resource = PydanticField(
        description="The underlying Boto3/S3 Dagster resource providing credentials and endpoint configuration."
    )
    s3_filter: Optional[str] = PydanticField(
        default=None,
        description="Optional regex filter used by sensors to match S3 keys."
    )


class S3SensorResource(ConfigurableResource, ConfigureFromDict):
    """Dagster resource providing wrapped Boto3 S3 polling operations.
    
    Integrates with Dagster's standard S3Resource to support custom endpoints (Minio),
    credentials, and regex-based key filtering.
    """
    config: S3ResourceConfig
    
    def get_client(self) -> Any:
        # Access the boto3 client directly from the wrapped S3Resource
        return self.config.s3_resource.get_client()

    def get_object_to_set_on_execution_context(self) -> Any:
        return self.get_client()

    @classmethod
    def configure(cls, config: Dict[str, Any]) -> "S3SensorResource":
        """Factory method to construct the resource from a dictionary."""
        return cls(config=S3ResourceConfig.model_validate(config))

    def apply_filter(self, key: str) -> bool:
        """Determines if a given S3 key should trigger an ingestion pipeline.
        
        Excludes directories and metadata flags by default, then applies the 
        configured regex filter if present.
        """
        # Exclude directories
        if key.endswith('/'):
            return False
            
        # Standard metadata/success ignores
        ignore_patterns = ["_SUCCESS", ".metadata"]
        if any(p in key for p in ignore_patterns):
            return False
            
        # Configured regex filter
        if self.config.s3_filter:
            return bool(re.match(self.config.s3_filter, key))
            
        return True

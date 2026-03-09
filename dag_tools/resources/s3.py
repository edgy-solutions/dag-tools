from typing import Any, Optional
import boto3
from dagster import ConfigurableResource
from pydantic import Field

class S3SensorConfig(ConfigurableResource):
    """Configuration specific to the Dagster S3 Sensor polling mechanism."""
    s3_bucket: str = Field(description="The AWS S3 bucket name to scan for incoming files.")
    s3_prefix: Optional[str] = Field(default=None, description="The S3 prefix folder path.")

class S3SensorResource(ConfigurableResource):
    """Dagster resource providing wrapped Boto3 S3 polling operations.
    Exposes filter functionality to ignore metadata files.
    """
    config: S3SensorConfig
    
    def get_client(self) -> Any:
        # Relies on the host environment's AWS permissions or standard boto3 env vars.
        return boto3.client("s3")
        
    def apply_filter(self, key: str) -> bool:
        """Determines if a given S3 key should trigger an ingestion pipeline."""
        # By default exclude directories and success flags
        if key.endswith('/'):
            return False
            
        ignore_patterns = ["_SUCCESS", ".metadata"]
        if any(p in key for p in ignore_patterns):
            return False
            
        return True

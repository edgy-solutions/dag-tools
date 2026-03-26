from typing import Any, Dict, Optional
from pydantic import Field

from dagster import (
    AssetSelection,
    Definitions,
    DynamicPartitionsDefinition,
    asset,
    define_asset_job,
    Config,
    AssetExecutionContext,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

class S3FileConfig(Config):
    file_url: str
    extra_metadata: Dict[str, Any] = {}

class S3ToFileComponent(Component, Resolvable, Model):
    """A declarative Dagster Component that defines a generic File Extraction Asset.
    Designed to receive S3 file URLs and pass them to downstream parsing/ML assets.
    """
    
    name: str = Field(description="The name of the asset.")
    partition_name: str = Field(description="The name of the dynamic partition definition.")
    op_config: Dict[str, Any] = Field(default_factory=dict, description="Static configuration to merge into the op's run config.")
    
    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.name
        job_name = f"ingest_files_{asset_name}"
        
        # 1. Define the dynamic partition mapping schema
        partitions_def = DynamicPartitionsDefinition(name=self.partition_name)
        
        # 2. Define the generic File Asset
        @asset(
            name=asset_name,
            compute_kind='file',
            partitions_def=partitions_def,
        )
        def s3_file_asset(context: AssetExecutionContext, config: S3FileConfig) -> Any:
            """Generic asset that 'materializes' a file reference."""
            file_url = config.file_url
            context.log.info(f"Received file for processing: {file_url}")
            
            # Record metadata about the file
            metadata = {
                "file_url": file_url,
                "partition": context.partition_key,
                **config.extra_metadata,
                **self.op_config
            }
            context.add_output_metadata(metadata)
            
            # Return the file URL or the metadata as the asset value
            return metadata

        # 3. Create the targeted Dagster Job
        ingest_job = define_asset_job(
            name=job_name,
            selection=AssetSelection.assets(s3_file_asset),
            partitions_def=partitions_def
        )
        
        # 4. Bind and aggregate definitions
        return Definitions(
            assets=[s3_file_asset],
            jobs=[ingest_job]
        )

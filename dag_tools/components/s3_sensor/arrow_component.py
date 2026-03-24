from typing import Any, Dict, Optional
from pydantic import Field

from dagster import (
    AssetSelection,
    Definitions,
    DynamicPartitionsDefinition,
    asset,
    define_asset_job,
)
from dagster.components import Component, ComponentLoadContext

from dag_tools.resources.arrow import ArrowResource

class S3ToArrowComponent(Component):
    """A declarative Dagster Component that defines a PyArrow Extraction Asset 
    matching a Dynamic Partition schema. Designed to be triggered by an S3SensorComponent.
    """
    
    partition_name: str = Field(description="The name of the dynamic partition definition (e.g., 'daily_logs').")
    asset_name: Optional[str] = Field(default=None, description="Optional override for the asset name.")
    io_manager_key: str = Field(default="io_manager", description="The IO Manager responsible for persisting the datasets.")
    delimiter: str = Field(default=",", description="CSV parsing delimiter if applicable.")
    
    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        clean_name = self.partition_name.replace("-", "_")
        asset_name = self.asset_name or f"{clean_name}_2arrow"
        job_name = f"ingest_s3_objects_{clean_name}"
        
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
        def s3_arrow_asset(asset_context, arrow_client: ArrowResource) -> Any:
            filename = asset_context.op_config["file_url"]
            delimiter = asset_context.op_config["delimiter"]
            partition = asset_context.asset_partition_key_for_output()
            
            asset_context.log.info(f"Ingesting partition {partition} from S3 URL: {filename}")
            # Ensure the pyarrow dataset abstracts S3 interactions globally
            return arrow_client.get_client().load_input_from_file(filename, asset_context.log, delimiter)

        # 3. Create the targeted Dagster Job that the sensor executes
        sensor_job = define_asset_job(
            name=job_name,
            selection=AssetSelection.assets(s3_arrow_asset),
            partitions_def=partitions_def
        )
        
        # 4. Bind and aggregate definitions
        return Definitions(
            assets=[s3_arrow_asset],
            jobs=[sensor_job],
            resources={
                "arrow_client": ArrowResource()
            }
        )

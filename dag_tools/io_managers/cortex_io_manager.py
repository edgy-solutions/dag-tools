import os
import polars as pl
from typing import Any, Optional
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    MetadataValue
)
from pydantic import Field
from dag_tools.cortex_data.client import CortexDataClient

class CortexPolarsIOManager(ConfigurableIOManager):
    """
    The Unified Cortex IO Manager.
    Forces Dagster to use the exact same CortexDataClient for load_input operations between assets,
    ensuring 100% uniformity across the mesh.
    """
    s3_bucket: str = Field(description="The S3 bucket for storing physical data.")
    prefix: str = Field(description="The S3 prefix (folder path) for storing physical data.")
    broker_url: str = Field(description="The Central Gateway URL.")
    client_id: str = Field(description="Machine-to-Machine OAuth2 Client ID.")
    client_secret: str = Field(description="Machine-to-Machine OAuth2 Client Secret.")
    keycloak_url: Optional[str] = Field(default=None, description="The Keycloak Token URL.")

    def load_input(self, context: InputContext) -> Any:
        """
        Triggered when a downstream Dagster asset needs to read an upstream asset.
        """
        # Extract the DataHub URN from the upstream asset's metadata (or deterministically generate it)
        urn = None
        
        # Check if the upstream output provided a datahub/urn metadata value
        if context.upstream_output and context.upstream_output.metadata:
            urn_meta = context.upstream_output.metadata.get("datahub/urn")
            if urn_meta:
                urn = str(urn_meta.value) if hasattr(urn_meta, "value") else str(urn_meta)
                
        if not urn:
            # Fallback deterministic URN generation
            key_str = context.asset_key.to_user_string()
            urn = f"urn:li:dataset:(urn:li:dataPlatform:dagster,{key_str.replace('/', '.')},PROD)"
            
        context.log.info(f"Loading input for URN: {urn}")
            
        # Instantiate the CortexDataClient using the M2M credentials
        client = CortexDataClient(
            broker_url=self.broker_url,
            client_id=self.client_id,
            client_secret=self.client_secret,
            keycloak_url=self.keycloak_url
        )
        
        # Call client.get_dataframe(urn)
        df = client.get_dataframe(urn)
        
        # Return the Polars LazyFrame directly to the Dagster compute function
        return df

    def handle_output(self, context: OutputContext, obj: Any):
        """
        Triggered when Dagster finishes computing an asset and needs to write it to disk.
        """
        if not isinstance(obj, (pl.DataFrame, pl.LazyFrame)):
            raise ValueError(f"Expected a Polars DataFrame or LazyFrame, got {type(obj)}")
            
        # Determine the physical S3 path
        s3_path = f"s3://{self.s3_bucket}/{self.prefix}/{context.asset_key.path[-1]}.parquet"
        
        context.log.info(f"Writing output to {s3_path}")
        
        # Assume the Dagster pod natively holds the AWS IAM Role necessary to write data.
        # Use Polars to write the data natively
        if isinstance(obj, pl.LazyFrame):
            obj.sink_parquet(s3_path)
        elif isinstance(obj, pl.DataFrame):
            obj.write_parquet(s3_path)
            
        # Generate the URN
        key_str = context.asset_key.to_user_string()
        urn = f"urn:li:dataset:(urn:li:dataPlatform:dagster,{key_str.replace('/', '.')},PROD)"
            
        # Add metadata to the materialization event that attaches the physical s3:// path 
        # and the DataHub URN to the asset's metadata. This ensures the Domain Broker sidecar can find it later!
        metadata = {
            "physical_uri": MetadataValue.path(s3_path),
            "datahub/urn": MetadataValue.text(urn)
        }
        
        context.add_output_metadata(metadata)

import io
import re
from typing import Any, Dict, Optional, Union

import pandas as pd
from dagster import (
    ConfigurableIOManager,
    ConfigurableResource,
    InputContext,
    Output,
    OutputContext,
)
from dagster._utils.cached_method import cached_method
from dagster_aws.s3.io_manager import PickledObjectS3IOManager, S3Resource
from pydantic import Field as PydanticField
from upath import UPath

from dag_tools.utils.helper import ConfigureFromDict


class FileObjectS3IOManager(PickledObjectS3IOManager):
    """An extension of the PickledObjectS3IOManager that handles raw file bytes.

    Instead of strictly pickling python objects, this manager writes raw bytes 
    directly to S3 when the object is a BytesIO stream. It also supports dynamic 
    file extensions based on Dagster output metadata.
    """

    def __init__(
        self,
        s3_bucket: str,
        s3_session: Any,
        s3_prefix: Optional[str] = None,
    ) -> None:
        super().__init__(
            s3_prefix=s3_prefix,
            s3_bucket=s3_bucket,
            s3_session=s3_session
        )

    def load_from_path(self, context: InputContext, path: UPath) -> bytes:
        """Loads the raw file bytes from S3.

        Args:
            context: The Dagster input context.
            path: The UPath representing the S3 location.

        Returns:
            The raw bytes of the file.
        """
        response = self.s3.get_object(Bucket=self.bucket, Key=str(path))
        return response["Body"].read()

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath) -> None:
        """Dumps a file or generic object to S3.

        If `file_ext` is present in the output metadata, it appends it to the path.
        If `obj` is io.BytesIO, it streams the bytes natively; otherwise, it pickles.

        Args:
            context: The Dagster output context.
            obj: The object or BytesIO stream to write.
            path: The UPath representing the destination S3 location.
        """
        # Append dynamic extension from metadata if present
        metadata = context.output_metadata or {}
        if file_ext := metadata.get("file_ext"):
            path = path.with_suffix(f".{file_ext.value}")

        if self.path_exists(path):
            context.log.warning(f"Removing existing S3 object: {path}")
            self.unlink(path)

        # Convert to bytes if not already a stream
        obj_bytes = obj if isinstance(obj, io.BytesIO) else io.BytesIO(obj)

        self.s3.upload_fileobj(obj_bytes, self.bucket, str(path))


class S3ResourceConfig(ConfigurableResource):
    """Configuration schema for S3-based resources and IO Managers."""
    
    s3_bucket: str = PydanticField(
        description="S3 bucket to use for the file manager."
    )
    s3_prefix: str = PydanticField(
        default="", 
        description="Prefix to use for the S3 bucket for this file manager."
    )
    s3_resource: S3Resource = PydanticField(
        description="The underlying Boto3/S3 Dagster resource."
    )
    s3_filter: Optional[str] = PydanticField(
        default=None,
        description="Optional regex filter used by sensors to match S3 keys."
    )


class S3SensorResource(ConfigurableResource, ConfigureFromDict):
    """A resource that encapsulates S3 client access and filtering for sensors."""

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
        """Determines if an S3 key matches the optional configured regex filter."""
        if not self.config.s3_filter:
            return True
        return bool(re.match(self.config.s3_filter, key))


class S3FileIOManager(ConfigurableIOManager, ConfigureFromDict):
    """Persistent IO manager using S3 for raw file storage.

    Assigns each op output to a unique filepath containing the run ID, step key, and output name.
    If the asset key has multiple components, the final component is used as the filename, 
    and preceding components acts as parent directories under the bucket prefix.

    Example Usage:
        ```python
        defs = Definitions(
            assets=[my_asset],
            resources={
                "io_manager": S3FileIOManager(
                    config=S3ResourceConfig(
                        s3_bucket="my-bucket",
                        s3_prefix="data/processed",
                        s3_resource=S3Resource(...)
                    )
                )
            }
        )
        ```
    """

    config: S3ResourceConfig

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    @cached_method
    def inner_io_manager(self) -> FileObjectS3IOManager:
        """Instantiates the underlying implementation of the IO manager lazily."""
        return FileObjectS3IOManager(
            s3_bucket=self.config.s3_bucket,
            s3_session=self.config.s3_resource.get_client(),
            s3_prefix=self.config.s3_prefix,
        )

    @classmethod
    def configure(cls, config: Dict[str, Any]) -> "S3FileIOManager":
        """Factory method to construct the IO Manager from a dictionary."""
        return cls(config=S3ResourceConfig.model_validate(config))

    def load_input(self, context: InputContext) -> bytes:
        return self.inner_io_manager().load_input(context)

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        self.inner_io_manager().handle_output(context, obj)


def pandas_to_excel_stream(df: pd.DataFrame, **to_excel_kwargs: Any) -> Output[io.BytesIO]:
    """Serializes a Pandas DataFrame into an Excel BytesIO stream wrapped in a Dagster Output.

    This helper function is designed to be returned directly from an `@asset` when 
    used in conjunction with an S3FileIOManager to stream the XLSX bytes to storage.

    Args:
        df: The pandas DataFrame to serialize.
        **to_excel_kwargs: Additional arguments to pass to `df.to_excel()`.

    Returns:
        A Dagster `Output` containing the BytesIO stream and metadata.
    """
    excel_stream = io.BytesIO()
    
    # Write the dataframe to the stream
    df.to_excel(excel_stream, **to_excel_kwargs)
    
    # Seek back to 0 so the IOManager can read it
    excel_stream.seek(0)
    
    return Output(
        value=excel_stream,
        metadata={
            "file_ext": "xlsx",
            "rows": len(df),
            "columns": len(df.columns),
        }
    )

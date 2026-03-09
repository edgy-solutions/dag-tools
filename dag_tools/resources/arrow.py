import pathlib
from typing import Optional

import pyarrow as pa
from pyarrow import csv
import pyarrow.dataset as ds
from dagster import ConfigurableResource


class ArrowClient:
    """Client for PyArrow dataset loading operations on S3 endpoints."""

    def __init__(
        self,
        uri_base: str,
        access_key_id: str,
        secret_access_key: str,
        end_point: str,
        region: str,
        allow_http: bool,
    ):
        self.uri_base = uri_base
        self.s3fs = pa.fs.S3FileSystem(
            access_key=access_key_id,
            secret_key=secret_access_key,
            endpoint_override=end_point,
            scheme="http" if allow_http else "https",
            region=region,
            allow_bucket_creation=True,
        )

    def load_input_from_file(self, filename: str, log, delimiter: str = ","):
        """Loads a file into an Arrow Dataset using the pre-configured S3 filesystem."""
        suffix = pathlib.Path(filename).suffix
        if suffix:
            suffix = suffix[1:]
            log.info(f"File type is {suffix}")
            
            if suffix == "gz":
                suffix = "csv"
                
            if suffix == "csv":
                format_obj = pa.dataset.CsvFileFormat(parse_options=csv.ParseOptions(delimiter=delimiter))
                schema = format_obj.inspect(f"{filename}", filesystem=self.s3fs)
                
                log.info(schema)
                for index, entry in enumerate(schema.types):
                    if entry.equals(pa.null()):
                        schema = schema.set(index, schema.field(index).with_type(pa.string()))
                        
                dataset = pa.dataset.dataset(
                    f"{filename}", schema=schema, filesystem=self.s3fs, format=format_obj
                )
            else:
                dataset = pa.dataset.dataset(f"{filename}", filesystem=self.s3fs, format=suffix)
                
            log.info(dataset.schema)
            return dataset
        
        # Fallback dataset handler if no extension provided
        return pa.dataset.dataset(f"{filename}", filesystem=self.s3fs)


class ArrowResource(ConfigurableResource):
    """Dagster resource providing a PyArrow client with S3 access configurations."""
    uri_base: str
    access_key_id: str
    secret_access_key: str
    end_point: str
    region: Optional[str] = "us-east-1"
    allow_http: Optional[bool] = False

    def get_client(self) -> ArrowClient:
        return ArrowClient(
            uri_base=self.uri_base,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
            end_point=self.end_point,
            region=self.region,
            allow_http=self.allow_http,
        )

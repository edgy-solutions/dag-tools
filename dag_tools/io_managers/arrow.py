from typing import Any, Dict, Literal, Optional, Union, get_args, get_origin

import fsspec
import pandas as pd
import pyarrow as pa
from pyarrow import csv
from pyarrow import dataset as ds
from pyarrow.fs import FileType, PyFileSystem, FSSpecHandler

from pydantic import Field
from upath import UPath
import s3fs

from dagster import (
    Config,
    ConfigurableIOManagerFactory,
    InputContext,
    MetadataValue,
    OutputContext,
    UPathIOManager,
)


formats = ['parquet', 'csv']
default_format = formats[0]


class S3FSCommonConfig(Config):
    access_key_id: str
    secret_access_key: str
    end_point: str
    region: Optional[str] = "us-east-1"
    allow_http: Optional[bool] = False


class FsspecS3FSConfig(Config):
    type_: Literal["fsspec.s3"] = 'fsspec.s3'
    common: S3FSCommonConfig
    cache_storage: str
    check_files: Optional[bool] = True


class S3FSConfig(Config):
    type_: Literal["s3"] = 's3'
    common: S3FSCommonConfig
    allow_bucket_creation: Optional[bool] = True


class LocalFSConfig(Config):
    type_: Literal["local"] = 'local'
    use_mmap: Optional[bool] = False


def fix_double_bucket_path(path: UPath) -> UPath:
    """Strips trailing duplicated bucket directories occasionally emitted by fsspec integrations."""
    path_str = str(path)
    drive = path.drive

    if not drive:
        return path

    duplicate_pattern = f"/{drive}/"
    start_index = path_str.find(duplicate_pattern)

    if start_index != -1:
        _, _, tail = path_str.rpartition(duplicate_pattern)
        return UPath(f"{path.protocol}://{drive}/{tail}")

    return path


class ArrowIOManager(UPathIOManager):
    """IOManager utilizing PyArrow to directly map S3 and Local UPaths to Arrow Datasets and Pandas Dataframes."""

    def __init__(
        self, 
        uri_base: str,
        config: Union[S3FSConfig, LocalFSConfig, FsspecS3FSConfig],
        load_csv_columns_as_strings: bool,
        load_csv_skip_rows: int
    ):
        if isinstance(config, FsspecS3FSConfig):
            s3 = s3fs.S3FileSystem(
                endpoint_url=config.common.end_point, 
                key=config.common.access_key_id, 
                secret=config.common.secret_access_key,
                use_ssl=not config.common.allow_http
            )
            self.s3fs = PyFileSystem(
                FSSpecHandler(
                    fsspec.filesystem(
                        "filecache", 
                        fs=s3, 
                        cache_storage=config.cache_storage, 
                        check_files=config.check_files
                    )
                )
            )  
        elif isinstance(config, S3FSConfig):    
            self.s3fs = pa.fs.S3FileSystem(
                access_key=config.common.access_key_id, 
                secret_key=config.common.secret_access_key, 
                endpoint_override=config.common.end_point,
                scheme='http' if config.common.allow_http else 'https',
                region=config.common.region,
                allow_bucket_creation=config.allow_bucket_creation
            )
        elif isinstance(config, LocalFSConfig):        
            self.s3fs = pa.fs.LocalFileSystem(use_mmap=config.use_mmap)
        else:        
            self.s3fs = pa.fs.LocalFileSystem()  
            
        self.uri_base = uri_base
        self.load_csv_columns_as_strings = load_csv_columns_as_strings
        self.load_csv_skip_rows = load_csv_skip_rows
        super().__init__(base_path=UPath(uri_base))


    def _load_from_path(self, context: InputContext, path: UPath) -> Any:
        fmt_str, clean_path = self._uri_and_format_for_path(path)
        schema = None
        
        # Handle CSV specific options and typing overrides
        if fmt_str == 'csv':
            read_options = csv.ReadOptions(skip_rows=self.load_csv_skip_rows) if self.load_csv_skip_rows else None
            format_obj = pa.dataset.CsvFileFormat(read_options=read_options)
            
            if self.load_csv_columns_as_strings:
                schema = format_obj.inspect(clean_path, filesystem=self.s3fs)
                for index, entry in enumerate(schema.types):
                    schema = schema.set(index, schema.field(index).with_type(pa.string()).with_name(schema.field(index).name.strip()))
        else:
            format_obj = pa.dataset.ParquetFileFormat()

        # Load as a single dataset (handles both single files OR directories natively without manual enumeration loops)
        dataset = pa.dataset.dataset(clean_path, filesystem=self.s3fs, format=format_obj, schema=schema)

        # Evaluate target type from context
        requested_type = context.dagster_type.typing_type
        origin = get_origin(requested_type)
        types = get_args(requested_type) if origin is Union else [requested_type]

        if pd.DataFrame in types:
            return dataset.to_table().to_pandas()
        if pa.Table in types:
            return dataset.to_table()
        
        return dataset


    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        path = fix_double_bucket_path(path)
        if context.dagster_type.typing_type == type(None):
            return None
            
        return self._load_from_path(context, path)


    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath) -> None:
        if not isinstance(obj, dict):
            obj = {'': obj}
            
        for key, item in obj.items():
            format, target_path = self._uri_and_format_for_path(path, key)
            
            if isinstance(item, (pd.DataFrame, pa.Table, pa.RecordBatch, pa.dataset.FileSystemDataset, pa.RecordBatchReader)):
                if not isinstance(item, pa.dataset.FileSystemDataset):
                    context.log.info(f"Row count: {len(item)}")
                pa.dataset.write_dataset(
                    item, 
                    target_path, 
                    format=format, 
                    filesystem=self.s3fs, 
                    existing_data_behavior='overwrite_or_ignore'
                )
            else:
                raise ValueError(f"Unsupported object type {type(item)} for ArrowIOManager.")

    def path_exists(self, path: UPath) -> bool:
        """Correctly supports Dagster memoization skips by assessing genuine existence via fs selector."""
        try:
            return self.s3fs.get_file_info(self._uri_for_path(path)).type != FileType.NotFound
        except Exception:
            return False

    def get_loading_input_log_message(self, path: UPath) -> str:
        return f"Loading Arrow Table from: {self._uri_for_path(path)}"

    def get_writing_output_log_message(self, path: UPath) -> str:
        return f"Writing Arrow Table at: {self._uri_for_path(path)}"

    def unlink(self, path: UPath) -> None:
        pass

    def make_directory(self, path: UPath) -> None:
        return None

    def get_metadata(self, context: OutputContext, obj: Any) -> Dict[str, MetadataValue]:
        path = self._get_path(context)
        return {"uri": MetadataValue.path(self._uri_for_path(path))}

    def get_op_output_relative_path(self, context: Union[InputContext, OutputContext]) -> UPath:
        return UPath("storage", super().get_op_output_relative_path(context))

    def _uri_and_format_for_path(self, path: UPath, key=None) -> tuple[str, str]:
        format = default_format
        if path.suffix and path.suffix[1:] in formats:
            format = path.suffix[1:]      
        return format, self._uri_for_path(path, key)

    def _uri_for_path(self, path: UPath, key=None) -> str:
        if key:
            path = path.joinpath(key)
        if not path.suffix:
            path = path.with_suffix(f'.{default_format}')
        return str(path).replace('s3://', '') 


class ConfigurableArrowIOManager(ConfigurableIOManagerFactory):
    fs: Union[S3FSConfig, LocalFSConfig, FsspecS3FSConfig] = Field(discriminator="type_")
    uri_base: str
    load_csv_columns_as_strings: Optional[bool] = False
    load_csv_skip_rows: Optional[int] = 0

    def create_io_manager(self, context) -> ArrowIOManager:
        return ArrowIOManager(
            uri_base=self.uri_base,
            config=self.fs,
            load_csv_columns_as_strings=self.load_csv_columns_as_strings,
            load_csv_skip_rows=self.load_csv_skip_rows
        )

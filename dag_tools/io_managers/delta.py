"""Delta Lake IO manager for Dagster assets backed by S3 or a local filesystem.

This module ships a Dagster ``IOManager`` that lets ``@asset`` definitions
read and write Delta Lake tables. It supports both the ``deltalake``
Python bindings (Rust-backed reader/writer) and the Polars-native Delta
scan/write path, with a choice of S3-access strategies for cases where
either bandwidth, caching, or filesystem-shape compatibility matters.

Why multiple S3 configurations
------------------------------
Delta on S3 can be accessed several different ways, each with its own
trade-offs:

  * **deltalake-rs storage_options** (``S3FSConfig``) — credentials passed
    directly to the Rust object_store crate. Fastest path; no extra
    Python-side filesystem indirection. Used when no caching is needed
    and the Rust client's S3 implementation suffices.

  * **deltalake-rs + FSSpec filecache** (``S3FSConfig`` with
    ``cache_storage``) — same Rust read path for the transaction log,
    but the actual Parquet data files are read through an FSSpec
    filecache to avoid re-downloading on subsequent reads. Wins big on
    re-reads of large tables in CI / sandbox runs.

  * **PyArrow S3FS** (``ArrowS3FSConfig``) — uses PyArrow's native S3
    client as the underlying filesystem. Useful when callers want the
    PyArrow ``FileSystem`` abstraction available downstream.

  * **FSSpec S3FS via PyArrow handler** (``FsspecS3FSConfig``) — wraps
    s3fs (the Python S3 filesystem) in a PyArrow ``FSSpecHandler``.
    Used when integrating with code that already speaks fsspec.

  * **Polars-native** (``PolarsS3FSConfig``) — bypasses the deltalake
    Python bindings entirely on the read path, using
    ``polars.scan_delta`` (and ``polars.DataFrame.write_delta`` on the
    write path). Smaller dependency surface when the consumer is
    Polars-native anyway.

  * **Local filesystem** (``LocalFSConfig``) — for development and tests.
    Not mesh-publishable: a remote broker can't serve a path on a
    Dagster pod's local filesystem.

Each config carries a ``type_`` ``Literal`` field so they form a
discriminated union in ``ConfigurableDeltaIOManager.fs`` — Dagster's
config system picks the right variant based on the ``type_`` value.

Mesh publishing
---------------
``DeltaIOManager.physical_coordinates`` implements the mesh-publishing
protocol: it returns the routing ticket a remote domain broker needs
to advertise these assets through the central gateway. The
``source_type`` is ``"s3_delta"`` for any S3-backed config; the cortex
data client's ``s3_delta`` dispatcher consumes the ticket with
``polars.scan_delta`` plus storage options derived from the credentials.
``LocalFSConfig`` assets return ``None`` from ``physical_coordinates``.

Type coercion
-------------
The IO manager inspects each input's declared Dagster type and
returns the right shape — pandas DataFrame, PyArrow Table, PyArrow
Dataset, Polars DataFrame, or Polars LazyFrame. Asset functions
declare their preferred type in the parameter annotation; this IO
manager honors that without the caller having to convert. ``Union``
type annotations are also honored — the first matching shape wins.
"""

import shutil
from typing import Any, Dict, List, Literal, Optional, Sequence, Union, get_args, get_origin

import fsspec
import pandas as pd
import polars as pl
import pyarrow as pa
import s3fs
from dagster import (
    Config,
    ConfigurableIOManagerFactory,
    InputContext,
    MetadataValue,
    OutputContext,
    UPathIOManager,
)
from dagster._check import CheckError
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from pyarrow import dataset as ds
from pyarrow.fs import FSSpecHandler, PyFileSystem
from pydantic import Field
from upath import UPath

from dag_tools.io_managers.column_schema import add_column_schema
from dag_tools.utils.helper import ConfigureFromDict


# Discriminator value the cortex data client uses to pick the
# ``scan_delta`` read path. Kept as a module constant so the IO
# manager's mesh-publishing output stays aligned with the client.
_DELTA_SOURCE_TYPE = "s3_delta"


class S3FSCommonConfig(Config):
    """S3 credentials and endpoint shared by every S3-backed Delta config.

    Pulled out into a separate ``Config`` class so each backend-specific
    config can compose it via a ``common`` field — keeps the credential
    surface uniform across backends and avoids drift between them.
    """

    access_key_id: str
    secret_access_key: str
    end_point: str
    region: Optional[str] = "us-east-1"
    allow_http: Optional[bool] = False


class FsspecS3FSConfig(Config):
    """S3 access via s3fs wrapped in a PyArrow ``FSSpecHandler``.

    Adds the FSSpec filecache layer, so repeated reads of the same
    table hit a local cache. ``cache_storage`` is the directory on the
    Dagster pod where cached blocks land; ``check_files`` controls
    whether the cache validates each access against the remote object's
    modification time.
    """

    type_: Literal["fsspec.s3"] = "fsspec.s3"
    common: S3FSCommonConfig
    cache_storage: str
    check_files: Optional[bool] = True


class ArrowS3FSConfig(Config):
    """S3 access via PyArrow's native ``S3FileSystem``.

    ``allow_bucket_creation`` controls whether the underlying client
    will create the destination bucket if it doesn't exist — useful
    in sandbox / test environments against a fresh MinIO instance.
    """

    type_: Literal["arrow.s3"] = "arrow.s3"
    common: S3FSCommonConfig
    allow_bucket_creation: Optional[bool] = True


class S3FSConfig(Config):
    """S3 access via deltalake-rs storage options (no extra Python FS).

    The simplest and usually-fastest backend. When ``cache_storage`` is
    set, reads are upgraded to go through an FSSpec filecache
    (transaction log still via deltalake-rs, data files via the cache),
    so repeated reads of the same table become local-disk reads.

    ``allow_unsafe_rename`` maps to deltalake-rs'
    ``AWS_S3_ALLOW_UNSAFE_RENAME`` — required for some non-AWS S3
    implementations (MinIO, Ceph) that don't support atomic
    multi-object renames.
    """

    type_: Literal["s3"] = "s3"
    common: S3FSCommonConfig
    allow_unsafe_rename: Optional[bool] = False
    cache_storage: Optional[str] = None
    check_files: Optional[bool] = True


class PolarsS3FSConfig(Config):
    """S3 access via the Polars-native Delta path (scan_delta / write_delta).

    Bypasses the deltalake Python bindings on both read and write —
    useful when the rest of the asset is Polars-native and there's
    no reason to round-trip through PyArrow. ``use_pyarrow_extension_array``
    controls whether Polars-to-pandas conversion preserves Arrow
    extension types.
    """

    type_: Literal["polars.s3"] = "polars.s3"
    common: S3FSCommonConfig
    use_pyarrow_extension_array: Optional[bool] = True
    allow_unsafe_rename: Optional[bool] = False
    cache_storage: Optional[str] = None
    check_files: Optional[bool] = True


class LocalFSConfig(Config):
    """Local filesystem backend — for development, tests, and ad-hoc runs.

    Not mesh-publishable: the broker is a remote pod and can't serve a
    path on the Dagster pod's local filesystem. ``physical_coordinates``
    returns ``None`` for this config and the broker skips the URN.
    """

    type_: Literal["local"] = "local"
    prefix: Optional[str] = None


# Discriminated union of every Delta backend the IO manager supports.
# The ``type_`` ``Literal`` on each member is the discriminator —
# Pydantic / Dagster's config system picks the right variant from a
# config dict by matching the value at ``type_``.
_DeltaFSConfig = Union[
    S3FSConfig,
    LocalFSConfig,
    ArrowS3FSConfig,
    FsspecS3FSConfig,
    PolarsS3FSConfig,
]


def _s3_storage_options(config) -> Dict[str, str]:
    """Build the deltalake-rs / object_store storage_options dict.

    These are the env-style keys the Rust object_store crate consumes
    on every read/write call; they're propagated through deltalake's
    ``storage_options`` parameter without further transformation.
    """
    return {
        "AWS_ACCESS_KEY_ID": config.common.access_key_id,
        "AWS_SECRET_ACCESS_KEY": config.common.secret_access_key,
        "AWS_ENDPOINT_URL": config.common.end_point,
        "AWS_ALLOW_HTTP": "true" if config.common.allow_http else "false",
        "AWS_S3_ALLOW_UNSAFE_RENAME": (
            "true" if getattr(config, "allow_unsafe_rename", False) else "false"
        ),
        "AWS_DEFAULT_REGION": config.common.region,
    }


def _s3_credentials_for_ticket(config) -> Dict[str, str]:
    """Build the credentials dict the cortex data client expects.

    The client's ``s3_delta`` dispatcher reads this dict to assemble
    Polars' ``storage_options`` for ``scan_delta``. Keys are the
    lowercase Polars-side names (``aws_access_key_id`` etc.) — distinct
    from the uppercase env-style names ``_s3_storage_options`` returns.
    """
    return {
        "aws_access_key_id": config.common.access_key_id,
        "aws_secret_access_key": config.common.secret_access_key,
        "aws_endpoint_url": config.common.end_point,
        "aws_region": config.common.region or "us-east-1",
    }


def _delta_ticket(
    config, uri_base: str, asset_key_path: Sequence[str]
) -> Optional[Dict[str, Any]]:
    """The mesh routing ticket for one asset.

    Module-level so the IO manager and its Configurable factory return
    exactly the same thing. Both have to expose ``physical_coordinates``
    — the broker reads whichever object sits in
    ``Definitions(resources=...)``, which is the factory — and computing
    the ticket in two places is how an advertised location drifts from
    the real one.
    """
    if isinstance(config, LocalFSConfig):
        # Local disk exists on one pod; the broker is a different pod.
        return None
    path = "/".join(asset_key_path)
    # No ``storage/`` segment. That prefix comes from
    # get_op_output_relative_path, which UPathIOManager uses for OP
    # outputs; assets go through get_asset_relative_path and land at
    # <uri_base>/<asset key>. The broker advertises assets, so the old
    # ``/storage/`` ticket pointed at a prefix that holds nothing --
    # consumers got "No files in log segment" from a route the gateway
    # served with full confidence.
    return {
        "source_type": _DELTA_SOURCE_TYPE,
        "physical_uri": f"{uri_base.rstrip('/')}/{path}",
        "credentials": _s3_credentials_for_ticket(config),
    }


class DeltaIOManager(UPathIOManager):
    """Dagster ``UPathIOManager`` that materializes assets as Delta tables.

    Storage backend is selected at construction time by config type;
    read / write dispatch is then driven by ``isinstance`` checks at
    each call. The IO manager honors the asset's declared Dagster type
    on input — pandas DataFrame, PyArrow Table, PyArrow Dataset, Polars
    DataFrame, or Polars LazyFrame — so consumers can ask for whichever
    shape is most efficient for their downstream code.
    """

    def __init__(self, config: _DeltaFSConfig, uri_base: str):
        self._config = config
        self._uri_base = uri_base

        # Deltalake-rs storage_options — only set for backends that
        # talk to deltalake-rs directly. Other backends route I/O
        # through ``self._s3fs`` instead.
        self._storage_options: Optional[Dict[str, str]] = None

        # PyArrow filesystem used for write_deltalake's ``filesystem``
        # argument and for DeltaTable.to_pyarrow_*. Set when the
        # selected config explicitly wants a PyArrow filesystem in the
        # path (``arrow.s3`` or ``fsspec.s3``).
        self._s3fs: Optional[Any] = None

        # Cached-read PyArrow filesystem. When ``cache_storage`` is
        # configured on ``S3FSConfig`` / ``PolarsS3FSConfig``, this
        # holds the FSSpec filecache wrapper that data-file reads go
        # through (the transaction log still routes via deltalake-rs).
        self._s3fs_patch: Optional[Any] = None

        # Every S3-backed backend needs storage_options, not just the ones
        # that read through deltalake-rs. Writes used to hand credentials
        # to write_deltalake as a PyArrow ``filesystem``; deltalake 1.x
        # removed that argument, so storage_options is now the only way
        # credentials reach a write. The PyArrow filesystems below are
        # still built — reads and the cache layer continue to use them.
        if isinstance(
            config,
            (S3FSConfig, PolarsS3FSConfig, FsspecS3FSConfig, ArrowS3FSConfig),
        ):
            self._storage_options = _s3_storage_options(config)

        if isinstance(config, (S3FSConfig, PolarsS3FSConfig)):
            if config.cache_storage:
                self._s3fs_patch = self._init_s3fs(config)
        elif isinstance(config, FsspecS3FSConfig):
            self._s3fs = self._init_s3fs(config)
        elif isinstance(config, ArrowS3FSConfig):
            self._s3fs = pa.fs.S3FileSystem(
                endpoint_override=config.common.end_point,
                scheme="http" if config.common.allow_http else "https",
                region=config.common.region,
                allow_bucket_creation=config.allow_bucket_creation,
            )
        elif isinstance(config, LocalFSConfig):
            if config.prefix:
                self._storage_options = {"PREFIX": config.prefix}

        super().__init__(base_path=UPath(uri_base))

    @staticmethod
    def _init_s3fs(config) -> PyFileSystem:
        """Build a PyArrow ``PyFileSystem`` wrapping an FSSpec filecache.

        Used by both the cached-read upgrade path (when an
        ``S3FSConfig`` / ``PolarsS3FSConfig`` has ``cache_storage``)
        and the standard ``FsspecS3FSConfig`` write path. The filecache
        keeps a local copy of each block; ``check_files=True`` makes
        the cache validate freshness against the remote on each access.
        """
        s3 = s3fs.S3FileSystem(
            endpoint_url=config.common.end_point,
            key=config.common.access_key_id,
            secret=config.common.secret_access_key,
            use_ssl=not config.common.allow_http,
        )
        return PyFileSystem(
            FSSpecHandler(
                fsspec.filesystem(
                    "filecache",
                    fs=s3,
                    cache_storage=config.cache_storage,
                    check_files=config.check_files,
                )
            )
        )

    @staticmethod
    def _coerce_from_delta_table(
        dt: DeltaTable, typing_type: Sequence[type], filesystem
    ) -> Any:
        """Convert a DeltaTable handle to the type requested by the asset.

        Looks up each candidate type in the asset's declared type set
        (including ``Union`` variants) and returns the first matching
        materialization. Falls back to returning the ``DeltaTable``
        handle unchanged when no known target type matches — lets the
        caller deal with conversion downstream.
        """
        if pd.DataFrame in typing_type:
            return dt.to_pandas(filesystem=filesystem)
        if pa.lib.Table in typing_type:
            return dt.to_pyarrow_table(filesystem=filesystem)
        if ds.Dataset in typing_type:
            return dt.to_pyarrow_dataset(filesystem=filesystem)
        if pl.LazyFrame in typing_type:
            return pl.scan_pyarrow_dataset(
                dt.to_pyarrow_dataset(filesystem=filesystem)
            )
        if pl.DataFrame in typing_type:
            return pl.scan_pyarrow_dataset(
                dt.to_pyarrow_dataset(filesystem=filesystem)
            ).collect()
        return dt

    @staticmethod
    def _coerce_from_polars(
        df: pl.LazyFrame, typing_type: Sequence[type], use_pyarrow_extension_array: bool
    ) -> Any:
        """Convert a Polars LazyFrame to the type requested by the asset.

        Used by the Polars-native read path (``_load_delta_polars``).
        ``use_pyarrow_extension_array`` is forwarded into the pandas
        conversion so extension types (decimal, datetime[tz], etc.)
        survive the round-trip.
        """
        if pd.DataFrame in typing_type:
            return df.to_pandas(use_pyarrow_extension_array=use_pyarrow_extension_array)
        if pa.lib.Table in typing_type:
            return df.to_arrow()
        if ds.Dataset in typing_type:
            return df.to_arrow()
        return df

    def _load_from_path_cached(
        self, dt: DeltaTable, typing_type: Sequence[type], filesystem
    ) -> Any:
        """Cached-read upgrade — bypass DeltaTable's own data-file reads.

        Uses ``dt.file_uris()`` to enumerate the data files that make up
        the current snapshot, then opens them as a PyArrow dataset
        through the cached filesystem. Subsequent reads of the same
        files hit the on-disk cache rather than re-downloading from S3.
        """
        uris = [uri.replace("s3://", "") for uri in dt.file_uris()]
        dataset = pa.dataset.dataset(
            uris, filesystem=self._s3fs_patch, format="parquet"
        )
        if pd.DataFrame in typing_type:
            return dataset.to_table().to_pandas()
        if pa.lib.Table in typing_type:
            return dataset.to_table()
        if ds.Dataset in typing_type:
            return dataset
        return dataset

    def _load_delta(
        self, context: InputContext, path: UPath, typing_type: Sequence[type]
    ) -> Any:
        """Read path via the deltalake Python bindings.

        Constructs a ``DeltaTable`` handle, then either takes the cached
        read upgrade (when ``cache_storage`` was configured) or falls
        through to deltalake's own to_pandas / to_pyarrow_* conversion.
        """
        dt = DeltaTable(path, storage_options=self._storage_options)
        filesystem = None
        if self._s3fs:
            filesystem = pa.fs.SubTreeFileSystem(path.path, self._s3fs)
        if self._s3fs_patch is not None:
            return self._load_from_path_cached(dt, typing_type, filesystem)
        return self._coerce_from_delta_table(dt, typing_type, filesystem)

    def _load_delta_polars(
        self, context: InputContext, path: UPath, typing_type: Sequence[type]
    ) -> Any:
        """Read path via ``polars.scan_delta`` — no deltalake bindings.

        Used when the configured backend is ``PolarsS3FSConfig``.
        Returns a Polars LazyFrame which is then coerced to the
        asset's declared type via ``_coerce_from_polars``.
        """
        df = pl.scan_delta(str(path), storage_options=self._storage_options)
        return self._coerce_from_polars(
            df,
            typing_type,
            self._config.use_pyarrow_extension_array
            if isinstance(self._config, PolarsS3FSConfig)
            else True,
        )

    @staticmethod
    def _dump_via_deltalake(
        context: OutputContext, path, item, storage_options
    ) -> None:
        """Write path via ``deltalake.writer.write_deltalake``.

        Accepts pandas DataFrames, PyArrow Tables / RecordBatches /
        RecordBatchReaders directly, plus Polars DataFrames (converted
        to Arrow first), plus PyArrow Datasets. Other types fail loudly
        rather than silently dropping data.

        ``mode='overwrite'`` plus ``schema_mode='overwrite'`` makes the
        writer accept schema evolution between materializations — the
        old table version is preserved in the transaction log, so
        previous snapshots remain accessible via time travel.

        Credentials travel in ``storage_options``, never as a PyArrow
        filesystem: ``write_deltalake`` took a ``filesystem`` argument
        until deltalake 1.x removed it (along with ``schema``, and
        ``overwrite_schema`` in favour of ``schema_mode``). Reads still
        accept ``filesystem`` — ``to_pandas`` / ``to_pyarrow_*`` all
        keep it — so the PyArrow-filesystem backends remain useful for
        reading and caching, and only the write side changed.
        """
        if isinstance(item, pl.LazyFrame):
            item = item.collect()
        if isinstance(item, pl.DataFrame):
            item = item.to_arrow()
        if isinstance(item, ds.Dataset):
            # A Dataset is lazy. Handing over a RecordBatchReader keeps it
            # that way -- the writer pulls batches as it goes -- and the
            # reader carries the schema, which is what the removed
            # ``schema=`` argument used to supply.
            item = item.scanner().to_reader()

        if not isinstance(
            item, (pd.DataFrame, pa.Table, pa.RecordBatch, pa.RecordBatchReader)
        ):
            raise ValueError(
                f"Unsupported object type {type(item)} for DeltaIOManager."
            )

        # A RecordBatchReader is a one-shot stream with no length, and
        # counting it here would drain it before the write.
        if hasattr(item, "__len__"):
            context.log.info(f"Row count: {len(item)}")

        # str(), not the UPath itself: deltalake accepts only str / Path /
        # DeltaTable. A LOCAL UPath subclasses pathlib.Path and slips
        # through, but an s3:// UPath does not — so passing the object
        # works on a local filesystem and raises "table_or_uri must be a
        # str, Path or DeltaTable" against real object storage.
        write_deltalake(
            str(path),
            item,
            storage_options=storage_options,
            mode="overwrite",
            schema_mode="overwrite",
        )

    @staticmethod
    def _dump_via_polars(
        context: OutputContext, path, item, storage_options
    ) -> None:
        """Write path via ``polars.DataFrame.write_delta``.

        LazyFrames are collected in streaming mode first to bound peak
        memory on large outputs. Anything that isn't a Polars DataFrame
        or LazyFrame fails loudly — this write path is opt-in via the
        ``PolarsS3FSConfig`` backend, and using a different shape there
        is almost always a misconfiguration.

        Schema evolution goes through ``delta_write_options`` rather than
        Polars' own ``overwrite_schema=``, which is deprecated. Note that
        everything in ``delta_write_options`` is forwarded verbatim to
        ``write_deltalake``, so a ``filesystem`` entry there raises the
        same TypeError as passing it directly — it cannot be used to
        smuggle a PyArrow filesystem back into the write.
        """
        if isinstance(item, pl.LazyFrame):
            item = item.collect(streaming=True)
        if isinstance(item, pl.DataFrame):
            # str() for the same reason as the deltalake path: an s3://
            # UPath is not a pathlib.Path and is rejected downstream.
            item.write_delta(
                str(path),
                mode="overwrite",
                storage_options=storage_options,
                delta_write_options={"schema_mode": "overwrite"},
            )
        else:
            raise ValueError(
                f"Unsupported object type {type(item)} for Polars DeltaIOManager."
            )

    def load_input(self, context: InputContext) -> Union[Any, Dict[str, Any]]:
        """Dagster input entry point.

        Tries the standard single-asset load path first; falls back to
        the multi-input load path (which Dagster's ``UPathIOManager``
        uses for assets with multiple upstream inputs to the same op)
        when the single-asset path can't pick a single ``UPath``.
        """
        try:
            return super().load_input(context)
        except CheckError:
            return self._load_multiple_inputs(context)

    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        """Read a single Delta table from ``path`` into the asset's type.

        Resolves the asset's declared Dagster type (including ``Union``
        annotations), then dispatches to either the Polars-native read
        path (when the backend is ``PolarsS3FSConfig``) or the
        deltalake-bindings read path.
        """
        if context.dagster_type.typing_type is type(None):
            return None
        path = self._uri_for_path(path)
        typing_type: List[type] = [context.dagster_type.typing_type]
        if get_origin(context.dagster_type.typing_type) is Union:
            typing_type = list(get_args(context.dagster_type.typing_type))
        if isinstance(self._config, PolarsS3FSConfig):
            return self._load_delta_polars(context, path, typing_type)
        return self._load_delta(context, path, typing_type)

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath) -> None:
        """Write a Delta table to ``path``.

        Wraps single-output values in a one-entry dict so the
        multi-output and single-output cases share the same write loop.
        Each entry composes its own destination against the base path
        — the loop never accumulates joinpaths across iterations.
        """
        if not isinstance(obj, dict):
            obj = {"": obj}
        base = self._uri_for_path(path)
        for key, item in obj.items():
            target_path = base.joinpath(key) if key else base
            # No PyArrow filesystem here: deltalake 1.x removed
            # write_deltalake's ``filesystem`` argument, so writes reach
            # object storage through storage_options alone. Reads still
            # use self._s3fs, which is why it is still built.
            if isinstance(self._config, PolarsS3FSConfig):
                self._dump_via_polars(
                    context, target_path, item, self._storage_options
                )
            else:
                self._dump_via_deltalake(
                    context, target_path, item, self._storage_options
                )

    def path_exists(self, path: UPath) -> bool:
        """Check whether a Delta table exists at ``path``.

        Used by Dagster's ``UPathIOManager.handle_output`` to decide
        whether to warn and call ``unlink`` before writing. We delegate
        to ``DeltaTable.is_deltatable``, which is the authoritative
        check — it inspects the path for a valid ``_delta_log/``
        directory rather than just probing for any file. Returns
        ``False`` on any error (auth, network, missing path) — the
        downstream write will surface the real problem.
        """
        try:
            return DeltaTable.is_deltatable(
                str(path), storage_options=self._storage_options
            )
        except Exception:
            return False

    def unlink(self, path: UPath) -> None:
        """Remove an existing Delta table at ``path``.

        Called by Dagster between materializations after ``path_exists``
        returns ``True``. The deltalake writer's ``mode='overwrite'``
        already replaces data on the next dump, but explicit removal
        ensures schema-evolution edge cases and orphan files from
        cancelled prior runs don't survive into the next materialization.

        Each backend uses the FS it already has wired:

          * ``LocalFSConfig`` — ``shutil.rmtree`` on the local path.
          * Configs with ``self._s3fs`` (PyArrow FS) — ``delete_dir`` on
            the same FS the IO manager uses for reads.
          * Plain deltalake-rs configs — build a transient ``s3fs``
            client from the configured credentials and remove
            recursively.

        Errors during delete are swallowed: an unlink on a path that
        doesn't exist isn't a failure mode worth surfacing, and any
        real problem (auth, network) re-surfaces immediately when
        the subsequent write fails.
        """
        uri = str(path)
        if isinstance(self._config, LocalFSConfig):
            shutil.rmtree(uri, ignore_errors=True)
            return
        if self._s3fs is not None:
            try:
                self._s3fs.delete_dir(uri.replace("s3://", ""))
            except (FileNotFoundError, OSError):
                pass
            return
        fs = s3fs.S3FileSystem(
            endpoint_url=self._config.common.end_point,
            key=self._config.common.access_key_id,
            secret=self._config.common.secret_access_key,
            use_ssl=not self._config.common.allow_http,
        )
        try:
            fs.rm(uri, recursive=True)
        except (FileNotFoundError, OSError):
            pass

    def get_loading_input_log_message(self, path: UPath) -> str:
        """Dagster log line emitted on read."""
        return f"Loading Deltatable from: {self._uri_for_path(path)}"

    def get_writing_output_log_message(self, path: UPath) -> str:
        """Dagster log line emitted on write."""
        return f"Writing Deltatable at: {self._uri_for_path(path)}"

    def make_directory(self, path: UPath) -> None:
        """No-op: Delta tables don't require pre-created directories.

        The deltalake writer creates the table directory and
        ``_delta_log/`` itself on first write; pre-creating would just
        leave an empty directory if the write then fails.
        """
        return None

    def get_metadata(
        self, context: OutputContext, obj: Any
    ) -> Dict[str, MetadataValue]:
        """Attach the Delta table URI and column schema as output metadata.

        The URI is surfaced in Dagit alongside the materialization event so
        users can copy the physical location of the table they just
        produced. The columns feed the DataHub catalog sensor, which turns
        them into a schemaMetadata aspect — this manager advertises its
        tables to the mesh via ``physical_coordinates``, so a consumer
        finding one in the catalog should be able to see its shape.
        """
        path = self._get_path(context)
        # str(), not the UPath: MetadataValue.path requires str | PathLike,
        # and an s3:// UPath is NOT PathLike (a local one is). Passing the
        # object therefore worked on a local filesystem and raised a
        # param-type mismatch against S3 -- after the data had already been
        # written, so the table existed but the step failed.
        metadata: Dict[str, MetadataValue] = {
            "uri": MetadataValue.path(str(self._uri_for_path(path)))
        }
        add_column_schema(metadata, obj)
        return metadata

    def get_op_output_relative_path(
        self, context: Union[InputContext, OutputContext]
    ) -> UPath:
        """Layout each asset under a ``storage/`` prefix beneath the base path.

        Keeps Delta data files separate from any other artifacts the
        IO manager's base path might hold — useful when ``uri_base``
        is a top-level bucket / prefix shared with other tools.
        """
        return UPath("storage", super().get_op_output_relative_path(context))

    def _uri_for_path(self, path: UPath) -> str:
        """Identity URI translation — kept as a hook for subclasses."""
        return path

    def _handle_transition_to_partitioned_asset(
        self, context: OutputContext, path: UPath
    ):
        """No-op — partitioned assets don't need special transition handling here."""
        pass

    def physical_coordinates(
        self, asset_key_path: Sequence[str]
    ) -> Optional[Dict[str, Any]]:
        """Mesh-publishing protocol — return the routing ticket for an asset.

        A remote broker registering this IO manager's assets with the
        central gateway calls this method to learn how to read each
        asset's data. The returned dictionary matches the ticket shape
        the cortex data client consumes: ``source_type`` discriminates
        the read path (``"s3_delta"`` here), ``physical_uri`` is the
        ``s3://`` URI of the Delta table, and ``credentials`` is the
        Polars-side storage-options dict the client forwards into
        ``scan_delta``.

        ``LocalFSConfig`` returns ``None`` — the broker is a remote pod
        and can't serve a path on the Dagster pod's local filesystem.
        S3-backed configs return a ticket pointing at
        ``{uri_base}/storage/{asset_key_path}``, mirroring the on-disk
        layout the IO manager itself uses (see
        ``get_op_output_relative_path``).
        """
        return _delta_ticket(self._config, self._uri_base, asset_key_path)


class ConfigurableDeltaIOManager(ConfigurableIOManagerFactory, ConfigureFromDict):
    """Dagster-native factory that constructs ``DeltaIOManager`` from config.

    The ``fs`` field is a discriminated union — Dagster's config system
    picks the right backend variant based on the ``type_`` value in the
    incoming config dict. ``uri_base`` is the root URI under which all
    assets bound to this IO manager are laid out.
    """

    fs: _DeltaFSConfig = Field(discriminator="type_")
    uri_base: str

    @classmethod
    def configure(cls, config) -> "ConfigurableDeltaIOManager":
        """Construct from a plain dict — used by ``ConfigureFromDict`` callers."""
        return cls.model_validate(config)

    def create_io_manager(self, context) -> DeltaIOManager:
        return DeltaIOManager(self.fs, self.uri_base)

    def physical_coordinates(
        self, asset_key_path: Sequence[str]
    ) -> Optional[Dict[str, Any]]:
        """Mesh-publishing protocol — see :func:`_delta_ticket`.

        This has to live on the FACTORY, not only on ``DeltaIOManager``.
        The domain broker looks up the object registered in
        ``Definitions(resources=...)`` and checks it for
        ``physical_coordinates``; that object is this factory, so with the
        method only on the inner manager the check failed and every Delta
        asset silently fell through to the broker's placeholder ticket
        (bucket ``my-data-lake``) — a routing entry that resolves to
        nothing.

        Computed from config rather than by building the IO manager:
        constructing one instantiates S3 filesystems and can create a
        local cache directory, which is far too much work for answering
        "where does this asset live".
        """
        return _delta_ticket(self.fs, self.uri_base, asset_key_path)

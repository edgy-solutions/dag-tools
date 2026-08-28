"""DuckDB IO manager — SQL-shaped assets that never materialize in RAM.

The complement to :mod:`dag_tools.io_managers.arrow`. Arrow writes frames
that already exist in memory; this one writes *queries*, letting DuckDB
stream the result from source to object storage without the rows passing
through Python.

An asset returns a ``DuckDBPyRelation`` — a lazy query — and the IO
manager writes it out::

    @asset(io_manager_key="duckdb_io")
    def orders(duck: DuckDBResource):
        con = duck.connect()          # NOT get_connection(); see below
        return con.sql("SELECT * FROM read_csv('s3://raw/orders.csv')")

Why this exists alongside Arrow
-------------------------------
Measured against MinIO at 5M rows (569 MB CSV -> filtered parquet), the
DuckDB writer beat routing the same query through Arrow on every axis:

    direct, memory_limit=256MB   5.53s   peak RSS +136 MB   42.8 MB out
    arrow  (RecordBatchReader)   8.12s   peak RSS +250 MB   56.8 MB out

The Arrow path still works and stays supported — it is the right choice
when an asset already holds a frame. This one is for the case where the
work is a query and materializing it is pure cost.

Output shape
------------
Writes a *directory* of ``data_N.parquet`` parts, matching Arrow's
layout so consumers cannot tell the two apart, and so a large asset
splits across files instead of producing one unwieldy object. See
``file_size_bytes``.

Connection lifetime
-------------------
A relation is lazy — nothing has executed when the asset returns, and the
write happens later, in ``handle_output``. The relation stays bound to
the connection that created it, so that connection must still be open
then. Use :meth:`DuckDBResource.connect`, whose lifetime you control, and
NOT ``get_connection()``, which closes on block exit. Getting this wrong
raises with an explanation rather than a bare DuckDB error.
"""
from typing import Any, Dict, List, Optional, Sequence

from dagster import (
    ConfigurableIOManagerFactory,
    InputContext,
    IOManager,
    MetadataValue,
    OutputContext,
)
from pydantic import Field

from dag_tools.io_managers.column_schema import add_column_schema
from dag_tools.io_managers.mesh_publishing import MeshPublishable
from dag_tools.resources.duckdb import DuckDBResource, duckdb_path

DEFAULT_FORMAT = "parquet"

# What this manager produces, in the vocabulary the cortex data client
# dispatches on. Used for BOTH the mesh routing ticket and the
# ``destination_name`` the catalog sensor reads, so the two cannot drift.
SOURCE_TYPE = "s3_parquet"


def _is_relation(obj: Any) -> bool:
    """Duck-typed so importing duckdb stays off the module load path.

    This module is imported when a code location boots; a heavyweight
    import there has previously blown the Dagster gRPC launch budget.
    """
    return type(obj).__name__ == "DuckDBPyRelation"


def split_endpoint_instance(endpoint_url: Optional[str]) -> Optional[str]:
    """The platform-instance name implied by an S3 endpoint.

    ``http://minio-svc.namespace.svc.cluster.local:9000`` -> ``minio-svc``.

    DataHub's s3 recipes set ``platform_instance`` to distinguish one
    object store from another, and the resulting dataset name is
    ``<platform_instance>.<bucket>/<key>``. Deriving the instance from the
    endpoint the IO manager is already configured with means the emitted
    identity and the crawled identity agree without a second place to
    configure it -- and a mismatch there produces two disconnected
    entities for one table, which is exactly the failure this avoids.
    """
    if not endpoint_url:
        return None
    host = endpoint_url.split("://", 1)[-1].split("/", 1)[0].split(":", 1)[0]
    return host.split(".", 1)[0] or None


def asset_uri(
    uri_base: str,
    asset_key_path: Sequence[str],
    directory: bool = True,
    key_encodes_location: bool = False,
) -> str:
    """Where this IO manager stores a given asset key.

    The single source of truth for the layout, used by the writer, by
    ``physical_coordinates``, and by callers that need to reason about the
    output without owning it (a freshness check, say). Keeping one
    function is deliberate: when the write path and the advertised path
    were computed separately, they drifted, and a routing ticket that
    points where the data isn't is worse than no ticket at all.

    No format suffix on the directory. It used to append ``.parquet`` to
    the leaf, giving ``.../p_cage.parquet/data_0.parquet`` -- which then
    leaked into the DataHub name and the Dagster key. The directory is a
    table, not a file, so it is named like one.

    ``key_encodes_location=True`` means the asset key is
    ``<platform_instance>/<bucket>/<path...>`` -- the convention that
    makes a Dagster key, a DataHub URN and an S3 path three views of one
    fact. The location then comes from the KEY, and ``uri_base``
    contributes only scheme/credentials; taking the path from both would
    double-encode the bucket.

    ``directory=True`` appends the trailing slash that marks a dataset
    directory of part files -- required, because a consumer's
    ``scan_parquet`` treats a slash-less S3 path as an object key.
    """
    path = list(asset_key_path)
    if key_encodes_location:
        if len(path) < 3:
            raise ValueError(
                f"key_encodes_location expects <instance>/<bucket>/<path...>, "
                f"got {'/'.join(path)!r}"
            )
        scheme = uri_base.split("://", 1)[0] if "://" in uri_base else "s3"
        bucket, rest = path[1], path[2:]
        base_bucket = _bucket_of(uri_base)
        if base_bucket and base_bucket != bucket:
            # Loud, because the silent version writes to the wrong bucket.
            raise ValueError(
                f"asset key names bucket {bucket!r} but uri_base points at "
                f"{base_bucket!r} ({uri_base}); refusing to guess"
            )
        uri = "/".join([f"{scheme}://{bucket}", *rest])
    else:
        uri = "/".join([uri_base.rstrip("/"), *path])
    return uri + "/" if directory else uri


def _bucket_of(uri_base: str) -> Optional[str]:
    if "://" not in uri_base:
        return None
    return uri_base.split("://", 1)[1].split("/", 1)[0] or None


class DuckDBIOManager(IOManager):
    """Writes DuckDB relations to object storage; reads them back lazily."""

    def __init__(
        self,
        resource: DuckDBResource,
        uri_base: str,
        file_size_bytes: Optional[str] = "128MB",
        compression: Optional[str] = None,
        partition_by: Optional[Sequence[str]] = None,
        key_encodes_location: bool = False,
    ):
        self.resource = resource
        self.uri_base = uri_base.rstrip("/")
        self.file_size_bytes = file_size_bytes
        self.compression = compression
        self.partition_by = list(partition_by) if partition_by else None
        self.key_encodes_location = key_encodes_location
        # Reads hand back lazy relations, so the connection behind them has
        # to outlive load_input. Held on the manager, which Dagster scopes
        # to the run.
        self._con: Any = None

    # -- paths -------------------------------------------------------------

    def _uri_for(self, context: Any) -> str:
        """Write target for a context. See :func:`asset_uri`.

        No trailing slash here: this is handed to DuckDB's writer, which
        creates the directory itself and rejects the slash form.
        """
        if context.has_asset_key:
            path = list(context.asset_key.path)
        else:
            path = list(context.get_identifier())
        return asset_uri(
            self.uri_base, path, directory=False,
            key_encodes_location=self.key_encodes_location,
        )

    def _connection(self) -> Any:
        if self._con is None:
            self._con = self.resource.connect()
        return self._con

    # -- write -------------------------------------------------------------

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        if obj is None:
            # Assets that write their own output and return None (or a
            # MaterializeResult) have nothing for us to store.
            return

        # DuckDB addresses local files by plain path but object stores by
        # URL, so a file:// uri_base (the usual local/dev configuration) has
        # to be converted; s3:// passes through untouched.
        uri = duckdb_path(self._uri_for(context))
        relation = self._as_relation(obj, uri)

        options: Dict[str, Any] = {"overwrite": True}
        if self.file_size_bytes:
            # Any value here makes DuckDB emit a directory of data_N.parquet
            # parts rather than a single file — which is the shape we want
            # unconditionally, so that a small asset and a large one look
            # the same to a reader and a large one can split.
            options["file_size_bytes"] = self.file_size_bytes
        if self.compression:
            options["compression"] = self.compression
        if self.partition_by:
            options["partition_by"] = self.partition_by

        context.log.info(f"Writing DuckDB relation to: {uri}")
        self._ensure_local_parent(uri)
        try:
            relation.write_parquet(uri, **options)
        except Exception as e:
            raise RuntimeError(self._write_failure_hint(uri, e)) from e

        metadata = self.get_metadata()
        metadata["uri"] = MetadataValue.path(uri)
        rows = self._row_count(uri)
        if rows is not None:
            metadata["dagster/row_count"] = MetadataValue.int(rows)
        # The relation already carries its resolved schema, so this costs
        # no query — see dag_tools.io_managers.column_schema.
        add_column_schema(metadata, relation)
        context.add_output_metadata(metadata)

    def _row_count(self, uri: str) -> Optional[int]:
        """Row count of what was just written, from the Parquet footer.

        Counting the relation before the write would execute the query
        twice, and counting after the write is nearly free: Parquet stores
        the row count in each file's footer, so this reads metadata rather
        than rescanning the data.

        Best-effort -- a missing row count costs a metadata field, so it
        must never fail the materialization that already succeeded.
        """
        try:
            target = f"{uri}/**/*.parquet" if self.file_size_bytes else uri
            (rows,) = self._connection().execute(
                "SELECT count(*) FROM read_parquet(?)", [target]
            ).fetchone()
            return int(rows)
        except Exception:
            return None

    def _as_relation(self, obj: Any, uri: str) -> Any:
        """Coerce what the asset returned into something DuckDB can write.

        A relation is the intended input and passes straight through — it
        carries its own connection, already configured for S3 by the
        resource that made it.

        In-memory frames are accepted too, because a pipeline will mix the
        two and forcing the author to switch IO managers mid-graph is worse
        than a cheap conversion. They go through this manager's own
        connection. Arrow conversion is zero-copy for most dtypes.
        """
        if _is_relation(obj):
            return obj

        con = self._connection()

        try:
            import polars as pl

            if isinstance(obj, pl.LazyFrame):
                obj = obj.collect()
            if isinstance(obj, pl.DataFrame):
                obj = obj.to_arrow()
        except ImportError:
            pass

        try:
            import pyarrow as pa

            if isinstance(obj, (pa.Table, pa.RecordBatchReader)):
                return con.from_arrow(obj)
        except ImportError:
            pass

        try:
            import pandas as pd

            if isinstance(obj, pd.DataFrame):
                return con.from_df(obj)
        except ImportError:
            pass

        raise TypeError(
            f"DuckDBIOManager cannot write {type(obj).__name__} to {uri}. "
            f"Return a DuckDBPyRelation (e.g. con.sql(...)) for the streaming "
            f"path, or a polars/pandas DataFrame or pyarrow Table."
        )

    @staticmethod
    def _ensure_local_parent(uri: str) -> None:
        """Create intermediate directories for local targets.

        DuckDB creates the leaf output directory but not its parents, so a
        key-prefixed asset (``sales/orders``) fails on a local filesystem
        with "cannot find the path specified". Object stores have no real
        directories, so this is a no-op there.

        Expects the DuckDB-form path: a ``file://`` URI still names a local
        directory that has to exist, so testing for ``://`` before the
        conversion would skip exactly the case that needs this.
        """
        if "://" in uri:
            return
        import pathlib

        pathlib.Path(uri).parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _write_failure_hint(uri: str, error: Exception) -> str:
        """Turn the two predictable failures into instructions.

        Both present as opaque DuckDB errors that give no hint the cause is
        how the asset acquired its connection.
        """
        detail = str(error)
        hint = ""
        if "closed" in detail.lower():
            hint = (
                " The relation's connection is already closed. A relation is "
                "lazy — it executes here, in handle_output, not when the asset "
                "returns — so the connection must still be open. Use "
                "DuckDBResource.connect() rather than get_connection(), which "
                "closes on block exit."
            )
        elif uri.startswith("s3://"):
            hint = (
                " If this is an authentication or httpfs error, the relation "
                "was likely built on a bare duckdb.connect() rather than a "
                "connection from DuckDBResource, so it carries no S3 "
                "credentials."
            )
        return f"DuckDB failed writing to {uri}: {detail}{hint}"

    # -- read --------------------------------------------------------------

    def load_input(self, context: InputContext) -> Any:
        """Return a lazy relation over the stored parquet.

        Lazy on purpose: a downstream asset can push its filters and
        projections into DuckDB rather than reading the whole dataset in
        to discard most of it.
        """
        uri = duckdb_path(self._uri_for(context.upstream_output or context))
        context.log.info(f"Reading DuckDB relation from: {uri}")
        con = self._connection()
        # A directory of parts is the normal shape, so always glob; DuckDB
        # accepts the pattern for the single-file case too.
        return con.read_parquet(f"{uri}/**/*.parquet")

    # -- metadata ----------------------------------------------------------

    def get_metadata(self) -> Dict[str, MetadataValue]:
        metadata: Dict[str, MetadataValue] = {}
        if self.uri_base.startswith("s3://"):
            # Declare what was written in this manager's own vocabulary --
            # the same source_type the mesh ticket carries. The catalog
            # sensor translates it into DataHub's naming; an IO manager has
            # no business knowing what DataHub calls things.
            metadata["destination_name"] = MetadataValue.text(SOURCE_TYPE)
        return metadata


class ConfigurableDuckDBIOManager(MeshPublishable, ConfigurableIOManagerFactory):
    """Config surface for :class:`DuckDBIOManager`."""

    duckdb: DuckDBResource
    uri_base: str
    file_size_bytes: Optional[str] = Field(
        default="128MB",
        description=(
            "Target size per part file. Setting any value makes DuckDB write a "
            "directory of data_N.parquet parts instead of one file, which is "
            "the shape we want unconditionally — it matches the Arrow IO "
            "manager, and lets a large asset split rather than produce one "
            "unwieldy object. Set to null for a single file."
        ),
    )
    compression: Optional[str] = Field(
        default=None, description="Parquet codec (snappy, zstd, gzip). DuckDB's default if unset."
    )
    key_encodes_location: bool = Field(
        default=False,
        description=(
            "The asset key is <platform_instance>/<bucket>/<path...>, so the "
            "physical location comes from the KEY and uri_base supplies only "
            "scheme and credentials. Makes the Dagster key, the DataHub URN "
            "and the S3 path three views of one fact, matching what a DataHub "
            "s3 recipe with the same platform_instance discovers."
        ),
    )
    # List, not Sequence: Dagster's config system rejects typing.Sequence.
    partition_by: Optional[List[str]] = Field(
        default=None,
        description="Hive-partition the output by these columns.",
    )

    def create_io_manager(self, context) -> DuckDBIOManager:
        return DuckDBIOManager(
            resource=self.duckdb,
            uri_base=self.uri_base,
            file_size_bytes=self.file_size_bytes,
            compression=self.compression,
            partition_by=self.partition_by,
            key_encodes_location=self.key_encodes_location,
        )

    def mesh_uri(self, asset_key_path: Sequence[str]) -> Optional[str]:
        """Mesh-publishing protocol (ADR-0001) — where this asset's bytes are.

        ``physical_coordinates`` comes from ``MeshPublishable``; this supplies
        the one part only this manager knows.

        Returns ``None`` — "don't advertise" — unless the output is
        genuinely readable by the cortex data client. An advertised but
        unreadable location is worse than an unadvertised asset, because
        the gateway will confidently route consumers to it.

        The advertised URI is the dataset directory and MUST carry a
        trailing slash. The client calls ``pl.scan_parquet(physical_uri)``
        verbatim; against S3 a slash-less path is treated as an object key
        and the HEAD returns 404, though it works locally either way — a
        difference only a real object store reveals. Same contract the
        Arrow manager advertises, so the two stay interchangeable.
        """
        if not self.uri_base.startswith("s3://"):
            # Local disk exists on one pod only.
            return None

        path = list(asset_key_path or [])
        if not path:
            return None

        # file_size_bytes off means a single object, so no trailing slash.
        return asset_uri(
            self.uri_base, path,
            directory=bool(self.file_size_bytes),
            key_encodes_location=self.key_encodes_location,
        )

    # Ticket assembly comes from MeshPublishable (ADR-0001). It previously
    # lived in a private ``_ticket`` here and in three near-identical forms in
    # arrow/sql/delta — a contract documented only by its implementations,
    # which is how a copy of one came to omit a property none of them recorded.
    #
    # There is no credentials hook, structurally. This used to advertise
    # ``self.duckdb.aws_access_key_id`` and its secret: the credential this IO
    # manager WRITES with, handed to any authorized reader with no expiry and
    # no scope. The broker mints per request instead (ADR-0044).

    def mesh_endpoint(self) -> Optional[str]:
        return self.duckdb.endpoint_url or None

    def mesh_region(self) -> Optional[str]:
        return self.duckdb.aws_region or "us-east-1"

"""DuckDB resource — a connection that can address object storage.

DuckDB's value in a pipeline is that it reads and writes ``s3://`` paths
directly: a multi-GiB CSV can be projected and written back as Parquet
without ever landing on the pod's disk, and without materializing in
Python memory. Getting a connection into that state is fiddly — the
``httpfs`` extension has to be present (which is its own problem in a
restricted cluster), and S3 credentials have to be pushed in through
``SET`` statements whose names differ from every other AWS client.

This resource owns that setup so assets don't repeat it. It is the
credential/driver boundary: the asset writes SQL, the resource knows how
to reach the backend.

Connection lifetime
-------------------
Use :meth:`DuckDBResource.get_connection` as a context manager for
self-contained work::

    with duckdb.get_connection() as con:
        con.execute("COPY (SELECT ...) TO 's3://bucket/out.parquet'")

If you hand a lazy object (a ``DuckDBPyRelation``, or an Arrow
``RecordBatchReader`` from ``fetch_arrow_reader()``) to something that
consumes it *later* — an IO manager's ``handle_output``, say — the
connection must outlive this scope, so do **not** close it. Use
:meth:`connect` instead, which returns an unmanaged connection whose
lifetime you control.
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional, Tuple
from urllib.parse import urlparse

from dagster import ConfigurableResource
from pydantic import Field

from dag_tools.utils.helper import ConfigureFromDict

logger = logging.getLogger(__name__)


def split_endpoint(endpoint_url: Optional[str]) -> Tuple[Optional[str], bool]:
    """Split ``http://minio:9000`` into ``("minio:9000", use_ssl=False)``.

    DuckDB wants the bare ``host:port`` in ``s3_endpoint`` plus a separate
    ``s3_use_ssl`` flag, whereas fsspec/boto want the full URL — so the
    same config value has to be reshaped for DuckDB specifically.
    """
    if not endpoint_url:
        return None, True
    parsed = urlparse(endpoint_url)
    if not parsed.netloc:
        return endpoint_url, True
    return parsed.netloc, parsed.scheme != "http"


def duckdb_path(url: str) -> str:
    """DuckDB addresses local files by plain path, but object stores by URL.

    ``file://`` URLs are converted to a plain path (including the Windows
    ``/C:/x`` case that ``urlparse`` produces); everything else — notably
    ``s3://`` — is passed through untouched.
    """
    if url.startswith("file://"):
        parsed = urlparse(url)
        path = (parsed.netloc or "") + parsed.path
        if os.name == "nt" and len(path) > 2 and path[0] == "/" and path[2] == ":":
            path = path[1:]
        return path
    return url


class DuckDBResource(ConfigurableResource, ConfigureFromDict):
    """A DuckDB connection wired for ``s3://`` access.

    Credentials live here rather than in assets — an asset should know
    the shape of its query, not how to authenticate to the backend.
    """

    aws_access_key_id: Optional[str] = Field(default=None)
    aws_secret_access_key: Optional[str] = Field(default=None)
    aws_region: str = Field(default="us-east-1")
    endpoint_url: Optional[str] = Field(
        default=None,
        description=(
            "Full endpoint URL (e.g. http://minio:9000). Split internally into "
            "DuckDB's s3_endpoint + s3_use_ssl. Leave unset for real AWS."
        ),
    )
    url_style: Optional[str] = Field(
        default=None,
        description=(
            "'path' or 'vhost'. Defaults to 'path' whenever an endpoint_url is "
            "set, because MinIO serves path-style buckets and virtual-host "
            "style resolves to a hostname that does not exist there."
        ),
    )
    extension_directory: Optional[str] = Field(
        default=None,
        description=(
            "Directory holding pre-baked DuckDB extensions. Defaults to the "
            "DUCKDB_EXTENSION_DIRECTORY env var. When set, the image is "
            "treated as self-contained: a failed httpfs load raises rather "
            "than silently reaching out to duckdb.org."
        ),
    )
    database: str = Field(
        default=":memory:",
        description="DuckDB database to open. In-memory suits pure ETL over object storage.",
    )
    memory_limit: Optional[str] = Field(
        default=None,
        description=(
            "DuckDB memory ceiling, e.g. '256MB' or '2GB'. Strongly "
            "recommended in a container: DuckDB's default is a fraction of "
            "detected system RAM, which can exceed the pod's cgroup limit and "
            "get the pod OOMKilled rather than spilling. Setting it is nearly "
            "free — measured at 5M rows, capping to 256MB cut peak RSS from "
            "434MB to 136MB for ~6% more wall time, because DuckDB spills to "
            "disk instead of buffering."
        ),
    )
    extra_settings: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional `SET <key>=<value>` statements applied after S3 config.",
    )

    @classmethod
    def configure(cls, config: Dict[str, Any]) -> "DuckDBResource":
        return cls.model_validate(config)

    # -- connection --------------------------------------------------------

    def connect(self) -> Any:
        """Return a configured, UNMANAGED connection.

        The caller owns the lifetime. Use this when a lazy object derived
        from the connection (a relation, or an Arrow ``RecordBatchReader``)
        outlives the calling scope — closing the connection first would
        break the consumer mid-stream.
        """
        import duckdb

        con = duckdb.connect(self.database)
        self._load_httpfs(con)
        self._configure_s3(con)
        if self.memory_limit:
            con.execute("SET memory_limit=?", [self.memory_limit])
        for key, value in (self.extra_settings or {}).items():
            con.execute(f"SET {key}=?", [value])
        return con

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        """Configured connection, closed on exit.

        Use for self-contained work (e.g. a ``COPY ... TO`` that completes
        inside the block). Do NOT use it to produce a lazy object you
        intend to consume later — see :meth:`connect`.
        """
        con = self.connect()
        try:
            yield con
        finally:
            con.close()

    @staticmethod
    def arrow_reader(relation: Any, batch_size: int = 100_000) -> Any:
        """Stream a relation out as a ``pyarrow.RecordBatchReader``.

        This is the hand-off to the Arrow IO manager: batches are pulled
        from DuckDB as the writer consumes them, so a result larger than
        RAM still writes. The reader stays bound to the connection, so
        whoever holds it must keep that connection open.

        DuckDB renamed ``fetch_arrow_reader`` to ``to_arrow_reader`` and
        deprecated the old name; both are in the wild across the versions
        our consumers pin, so pick whichever exists.
        """
        method = getattr(relation, "to_arrow_reader", None) or relation.fetch_arrow_reader
        return method(batch_size)

    # -- internals ---------------------------------------------------------

    def _load_httpfs(self, con: Any) -> None:
        """Make the connection able to address ``s3://``.

        In a container the extension is baked in at build time and
        ``extension_directory`` points at it. When that is set the image is
        meant to be self-contained, so a load failure is raised immediately
        rather than silently reaching for duckdb.org — which in a
        restricted cluster means a long hang followed by a confusing
        error, once per asset. Outside the image, a download is a
        reasonable convenience.
        """
        import duckdb

        directory = self.extension_directory or os.environ.get(
            "DUCKDB_EXTENSION_DIRECTORY"
        )
        if directory:
            con.execute("SET extension_directory=?", [directory])
            con.execute("SET autoinstall_known_extensions=false")
            try:
                con.execute("LOAD httpfs")
            except Exception as e:
                raise RuntimeError(
                    f"DuckDB {duckdb.__version__} could not load the httpfs "
                    f"extension from {directory}, which is required to read and "
                    f"write s3:// paths. The image is supposed to ship it — "
                    f"rebuild so the extension-baking step runs against this "
                    f"DuckDB version, since extensions are version- and "
                    f"architecture-specific and an upgraded duckdb will not "
                    f"find an extension baked for the previous one. "
                    f"Underlying error: {e}"
                ) from e
            return

        try:
            con.execute("LOAD httpfs")
        except Exception:
            try:
                con.execute("INSTALL httpfs")
                con.execute("LOAD httpfs")
            except Exception as e:
                raise RuntimeError(
                    "DuckDB could not load or install the httpfs extension, "
                    "which is required to read and write s3:// paths. Set "
                    "extension_directory (or DUCKDB_EXTENSION_DIRECTORY) to a "
                    "directory of pre-baked extensions to provide it offline. "
                    f"Underlying error: {e}"
                ) from e

    def _configure_s3(self, con: Any) -> None:
        """Push S3 settings in through DuckDB's ``SET`` interface."""
        if self.aws_access_key_id:
            con.execute("SET s3_access_key_id=?", [self.aws_access_key_id])
        if self.aws_secret_access_key:
            con.execute("SET s3_secret_access_key=?", [self.aws_secret_access_key])
        con.execute("SET s3_region=?", [self.aws_region or "us-east-1"])

        endpoint, use_ssl = split_endpoint(self.endpoint_url)
        if endpoint:
            con.execute("SET s3_endpoint=?", [endpoint])
            con.execute("SET s3_use_ssl=?", [use_ssl])
            # MinIO serves path-style buckets; virtual-host style resolves to
            # a hostname that does not exist there.
            con.execute("SET s3_url_style=?", [self.url_style or "path"])
        elif self.url_style:
            con.execute("SET s3_url_style=?", [self.url_style])

    @classmethod
    def from_env(cls, **overrides: Any) -> "DuckDBResource":
        """Build from the standard AWS environment variables.

        Convenience for deployments that already inject AWS_* into the pod
        (which is how the dag-tools user-deployment and broker are wired).
        """
        params: Dict[str, Any] = {
            "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
            "aws_region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            "endpoint_url": os.environ.get("AWS_ENDPOINT_URL"),
        }
        params.update(overrides)
        return cls(**params)

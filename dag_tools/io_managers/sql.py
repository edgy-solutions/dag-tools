"""SQL IO manager for Dagster assets backed by a SQL database.

This module ships a Dagster ``IOManager`` that lets ``@asset`` definitions
read and write pandas DataFrames against an arbitrary SQL database
(PostgreSQL, ClickHouse, MySQL, etc.). It is the SQL counterpart to the
S3-backed IO managers elsewhere in this package.

Data flow at a glance
---------------------
Reads use `connectorx <https://github.com/sfu-db/connector-x>`_ for its
parallel-bulk-read performance: connectorx pulls table data in chunks
across multiple connections and assembles the result as a pandas
DataFrame in one shot. Writes use ``pandas.DataFrame.to_sql`` (which goes
through SQLAlchemy under the hood) — connectorx itself is read-only.

Asset key to (schema, table) mapping
------------------------------------
Each Dagster asset has a key path like ``["sales", "customers"]``. The
SQL IO manager turns that path into a fully-qualified table identifier
through a small registry of "source resolver" functions:

  * The default resolver (``get_source_fn_default``) treats the first
    path segment as the schema name and joins remaining segments with
    underscores to form the table name.
  * Projects with non-standard naming conventions (CamelCase tables,
    different schema layouts, environment-prefixed table names, etc.)
    can register their own resolver via ``set_source_fn_for_asset_class``
    and reference it by name in ``SQLConfig.get_source_fn``.

The registry is module-level so a resolver registered at import time is
visible to every ``SQLIOManager`` instance in the process. This is
deliberate: a project typically has one naming convention shared
across many assets.

Mesh publishing
---------------
The IO manager implements the mesh-publishing protocol via
``SQLIOManager.physical_coordinates``: a remote domain broker (in the
mesh architecture) can ask the IO manager for the routing ticket needed
to read each asset, then expose the URN through the central gateway.
Only protocols the cortex data client knows how to read (PostgreSQL and
ClickHouse today) produce a ticket; other SQL dialects work fine for
Dagster's own IO but return ``None`` from ``physical_coordinates`` so
the broker skips them rather than silently advertising a backend the
client can't actually use.
"""

import logging
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

import pandas as pd
import connectorx as cx
from dagster import (
    Config,
    ConfigurableIOManagerFactory,
    InputContext,
    IOManager,
    OutputContext,
)
from pydantic import Field

from dag_tools.io_managers.column_schema import add_column_schema
from dag_tools.utils.helper import ConfigureFromDict

logger = logging.getLogger(__name__)


# Module-level registry of asset-key-path resolvers, keyed by a short
# string name. Populated through ``set_source_fn_for_asset_class``.
# ``SQLConfig.get_source_fn`` references entries here by name; on IO
# manager construction the matching callable is looked up and replaces
# the default resolver for all assets bound to that IO manager.
_source_fns: Dict[str, Callable[[Sequence[str]], Optional[str]]] = {}


# Mapping from the URI scheme stored in ``SQLConfig.protocol`` to the
# ``source_type`` discriminator the cortex data client dispatches on.
# Two read paths are wired in the client today: PostgreSQL via ADBC,
# and ClickHouse via the clickhouse-connect Arrow path. When a new read
# path lands in the client, add the matching entry here so the IO
# manager will start publishing those assets through the broker; until
# then ``physical_coordinates`` returns ``None`` for unmapped protocols.
_DIALECT_TO_SOURCE_TYPE: Dict[str, str] = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "clickhouse": "clickhouse",
}


def get_source_fn_default(path: Sequence[str]) -> str:
    """Default mapping from an asset key path to a ``schema.table`` string.

    The first path segment is treated as the schema name when the path
    has more than one segment; the remaining segments are joined with
    underscores to form the table name. A single-segment path is
    returned as the table name only — the SQL IO manager's configured
    ``schema_`` then fills in as the schema at write/read time.

    Examples::

        ["customers"]              -> "customers"
        ["sales", "customers"]     -> "sales.customers"
        ["sales", "raw", "events"] -> "sales.raw_events"
    """
    if len(path) > 1:
        schema = path[0]
        table = "_".join(path[1:])
        return f"{schema}.{table}"
    return path[0]


class SQLConfig(Config):
    """Connection configuration for ``SQLIOManager``.

    Attributes:
        protocol: SQL URI scheme — e.g. ``"postgres"``, ``"postgresql"``,
            ``"clickhouse"``, ``"mysql"``. Drives both the SQLAlchemy
            URI on the write path and the connectorx read path. Also
            determines whether the IO manager is mesh-publishable: only
            protocols in ``_DIALECT_TO_SOURCE_TYPE`` produce a routing
            ticket from ``physical_coordinates``.
        host: Database host or service name.
        port: Database port. ``None`` is allowed for cases where the
            URI shouldn't include an explicit port (some libpq configs).
        database: Target database / catalog name.
        schema_: Default schema for unqualified table references. The
            public field name is ``schema``; we use ``schema_`` because
            ``schema`` is reserved in Pydantic's own metadata. The
            ``Field(alias="schema")`` keeps the wire / config-file name
            as ``schema``.
        username: Database user.
        password: Database password. Redacted from log output by the
            IO manager's initializer.
        get_source_fn: Name of a resolver to look up in ``_source_fns``.
            ``None`` (the default) falls back to ``get_source_fn_default``.
        write_style: ``pandas.to_sql`` ``if_exists`` argument — one of
            ``"replace"``, ``"append"``, ``"fail"``.
    """

    protocol: str
    host: str
    port: Optional[int] = None
    database: str
    schema_: Optional[str] = Field(
        default=None, alias="schema", description="Default schema for unqualified tables."
    )
    username: str
    password: str
    get_source_fn: Optional[str] = None
    write_style: str = "replace"


class SQLIOManager(IOManager):
    """Dagster ``IOManager`` that materializes assets as rows in SQL tables.

    Each asset's key path maps to a (schema, table) pair through the
    resolver function selected by ``SQLConfig.get_source_fn``. On output,
    a pandas DataFrame is written via ``DataFrame.to_sql``; on input,
    the table is bulk-loaded via connectorx and returned as a new
    pandas DataFrame. Per-input metadata can drive column projection
    and row filtering at SELECT time — see ``load_input`` for the
    metadata keys.
    """

    def __init__(self, config: SQLConfig):
        self._config = config

        # Build the connection URI used by both connectorx (reads) and
        # SQLAlchemy under pandas.to_sql (writes). ``port`` is allowed
        # to be omitted; when it is, we leave it out of the URI rather
        # than emitting ``host:`` with an empty port.
        port = config.port if config.port is not None else ""
        host_port = f"{config.host}:{port}" if port != "" else config.host
        self._con = (
            f"{config.protocol}://{config.username}:{config.password}"
            f"@{host_port}/{config.database}"
        )

        self._default_schema = config.schema_

        # Resolve which source-name → ``schema.table`` function to use.
        # An empty / unknown key falls back to the default resolver so
        # mis-typed names degrade to a sensible default rather than
        # crashing at construction time.
        self._get_source_fn = _source_fns.get(
            config.get_source_fn or "", get_source_fn_default
        )
        self.write_style = config.write_style

        # Log the resolved connection with the password redacted so
        # operators can confirm host/db/user without leaking secrets to
        # log aggregators.
        logger.info(
            "SQLIOManager initialized with connection %s",
            self._con.replace(config.password, "*******"),
        )
        logger.info(
            "SQLIOManager source resolver: %s (%s)",
            config.get_source_fn or "default",
            self._get_source_fn.__module__,
        )

    def _split_source(self, source: str) -> Tuple[Optional[str], str]:
        """Split a resolver output (``"schema.table"``) into its parts.

        When the source contains no dot, the IO manager's configured
        default schema is used. Multi-dot identifiers (``db.schema.table``)
        keep the first segment as the schema and fold the rest back
        into the table name — this preserves the behavior expected by
        callers using fully-qualified three-part names.
        """
        cleaned = source.replace('"', "")
        parts = cleaned.split(".")
        if len(parts) == 1:
            return self._default_schema, parts[0]
        return parts[0], ".".join(parts[1:])

    def handle_an_output(
        self, context: OutputContext, obj: pd.DataFrame, path: Sequence[str]
    ) -> None:
        """Write a single DataFrame to the table derived from ``path``.

        ``path`` is the asset key path (or, for partitioned assets, the
        partition key wrapped in a list — see ``handle_output``). It
        flows through the configured source resolver to produce a
        ``"schema.table"`` string, which is then split into the
        ``pandas.to_sql`` ``schema`` / ``name`` arguments.
        """
        source = self._get_source_fn(path)
        schema, table = self._split_source(source)
        obj.to_sql(
            name=table, schema=schema, con=self._con, if_exists=self.write_style
        )

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Dagster output entry point.

        Dispatches by the output type:

          * ``None`` — assume the work has already been done out-of-band
            (e.g. a dbt model that wrote the table itself); nothing to
            do here.
          * ``pandas.DataFrame`` — single-table write. For partitioned
            assets, the partition key becomes the table name and the
            default schema applies; for unpartitioned assets, the asset
            key path flows through the source resolver.
          * ``Mapping`` — multi-table write. Each ``(path, DataFrame)``
            entry produces one table write, independently resolved.

        Any other object type is rejected — DataFrame-shaped data is the
        only contract this IO manager can write.
        """
        if obj is None:
            return
        if isinstance(obj, pd.DataFrame):
            if context.has_asset_partitions:
                path = [context.asset_partition_keys[0]]
            else:
                path = context.asset_key.path
            self.handle_an_output(context, obj, path)
            self._emit_column_schema(context, obj)
            return
        if isinstance(obj, Mapping):
            for p, o in obj.items():
                self.handle_an_output(context, o, p)
            self._emit_column_schema(context, obj)
            return
        raise ValueError(f"Unsupported object type {type(obj)} for SQLIOManager.")

    @staticmethod
    def _emit_column_schema(context: OutputContext, obj: Any) -> None:
        """Publish the written table's columns as output metadata.

        This manager advertises its tables to the mesh through
        ``physical_coordinates``, so a consumer that finds one in the
        DataHub catalog should be able to see its shape — the catalog
        sensor turns this metadata into a schemaMetadata aspect.

        Emitted once per output, after the writes: metadata can only be
        set once per output, so the multi-table case describes the first
        DataFrame rather than calling this per table.
        """
        metadata: Dict[str, Any] = {}
        add_column_schema(metadata, obj)
        if metadata:
            try:
                context.add_output_metadata(metadata)
            except Exception:
                # Never fail a write whose data already landed.
                pass

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a SQL table as a pandas DataFrame using connectorx.

        The asset key path flows through the configured source resolver
        to produce the ``FROM`` clause. Per-input metadata supports
        column projection and row filtering at SELECT time so the IO
        manager doesn't materialize unneeded data:

          * ``select`` — comma-separated column list (default ``"*"``).
          * ``where`` — SQL ``WHERE`` expression without the ``WHERE``
            keyword.

        Example asset usage::

            @asset(ins={"customers": AssetIn(metadata={
                "select": "id, name, region",
                "where": "region = 'US'",
            })})
            def us_customer_summary(customers: pd.DataFrame) -> pd.DataFrame:
                ...
        """
        metadata = context.definition_metadata or {}
        select = metadata.get("select", "*")
        where_clause = metadata.get("where")
        where = f"WHERE {where_clause}" if where_clause else ""
        source = self._get_source_fn(context.asset_key.path)
        return cx.read_sql(
            query=f"SELECT {select} FROM {source} {where}", conn=self._con
        )

    def physical_coordinates(
        self, asset_key_path: Sequence[str]
    ) -> Optional[Dict[str, Any]]:
        """Mesh-publishing protocol — return the routing ticket for an asset.

        A remote broker registering this IO manager's assets with the
        central gateway calls this method to learn how to read each
        asset's data. The returned dictionary matches the ticket shape
        the cortex data client consumes from the gateway's authorize
        response: ``source_type`` discriminates the read path,
        ``physical_uri`` is parsed for host / schema / table, and
        ``credentials`` is forwarded into the client's database driver.

        Returns ``None`` when this IO manager's protocol isn't one the
        client knows how to read. The broker treats ``None`` as "skip
        this URN" rather than guessing a default backend — if a new
        protocol is meaningful to advertise through the mesh, add it to
        ``_DIALECT_TO_SOURCE_TYPE`` first.
        """
        source_type = _DIALECT_TO_SOURCE_TYPE.get(self._config.protocol)
        if not source_type:
            return None
        source = self._get_source_fn(asset_key_path)
        schema, table = self._split_source(source)

        # Mirror the connection-URI assembly used for the IO manager's
        # own read/write path so the broker advertises the same host
        # and port the IO manager would use internally.
        port = self._config.port if self._config.port is not None else ""
        host_port = (
            f"{self._config.host}:{port}" if port != "" else self._config.host
        )

        # The physical_uri layout below is what the cortex data client's
        # PostgreSQL / ClickHouse dispatchers parse — keep this format
        # in sync with those dispatchers if either side changes.
        physical_uri = (
            f"{source_type}://{host_port}/{schema or 'public'}/{table}"
        )
        return {
            "source_type": source_type,
            "physical_uri": physical_uri,
            "credentials": {
                "username": self._config.username,
                "password": self._config.password,
                "database": self._config.database,
            },
        }


def set_source_fn_for_asset_class(
    asset_class: str,
    get_source_fn: Callable[[Sequence[str]], Optional[str]],
) -> None:
    """Register a source-path resolver under a name.

    Once registered, the resolver becomes selectable by setting
    ``SQLConfig.get_source_fn`` to the same ``asset_class`` string.
    Resolvers should be registered at module import time so they're
    available when Dagster builds the IO manager — registering after
    Dagster has already instantiated the IO manager has no effect on
    existing instances (each instance binds its resolver in ``__init__``).
    """
    logger.info(
        "SQLIOManager source resolver registered for %s: %s",
        asset_class,
        get_source_fn.__module__,
    )
    _source_fns[asset_class] = get_source_fn


class ConfigurableSQLIOManager(ConfigurableIOManagerFactory, ConfigureFromDict):
    """Dagster-native factory that constructs ``SQLIOManager`` from config.

    Used in ``Definitions(resources={...})`` to wire the IO manager
    against a ``SQLConfig`` populated from Dagster's run config or a
    plain dictionary (via ``ConfigureFromDict``).
    """

    config: SQLConfig

    @classmethod
    def configure(cls, config: Mapping[str, Any]) -> "ConfigurableSQLIOManager":
        """Construct from a plain dict — used by ``ConfigureFromDict`` callers."""
        return cls.model_validate(config)

    def create_io_manager(self, context) -> SQLIOManager:
        return SQLIOManager(self.config)

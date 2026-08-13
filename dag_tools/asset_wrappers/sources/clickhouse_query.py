"""A dlt source for reading ClickHouse, including OpenTelemetry tables.

Why a custom source rather than ``dlt.sources.sql_database``: dlt treats
ClickHouse as a *destination* in this repo (see
``dag_tools/asset_wrappers/dlt_assets_parsing.py``), and the SQLAlchemy
path would need a ClickHouse dialect this project does not depend on.
``clickhouse-connect`` is already a first-class dependency and is the
official driver, so extraction goes straight through it.

Reading OTel tables specifically: ``ResourceAttributes`` /
``SpanAttributes`` / ``LogAttributes`` are ``Map(String, String)`` and
come back from clickhouse-connect as real dicts. They are preserved as
dicts here rather than flattened, because the mapping layer
(``dag_tools.otel_api_sync``) resolves attribute keys out of them by
name — flattening would force every mapping expression to know the
column layout.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterator, List, Optional

import dlt

logger = logging.getLogger(__name__)

# Attribute columns kept as JSON by default.
#
# This default is load-bearing, not cosmetic. Left to itself dlt
# *flattens* a nested dict into `span_attributes__item_name` columns and
# lower-cases every part, which loses the original attribute casing —
# `metric.NUM_ERROR` comes back as `num_error`, so anything reconstructing
# names from columns (metric arrays) is silently wrong. Hinting the column
# as JSON keeps the map intact through the staging load.
DEFAULT_MAP_COLUMNS = ("ResourceAttributes", "SpanAttributes", "LogAttributes", "Attributes")


def _client(connection: Dict[str, Any]):
    """Open a clickhouse-connect client from a normalized config dict."""
    import clickhouse_connect

    kwargs: Dict[str, Any] = {
        "host": connection.get("host", "localhost"),
        "username": connection.get("username") or connection.get("user") or "default",
        "password": connection.get("password", ""),
        "database": connection.get("database", "default"),
    }
    if connection.get("port"):
        kwargs["port"] = int(connection["port"])
    if connection.get("secure") is not None:
        kwargs["secure"] = bool(connection["secure"])
    if connection.get("settings"):
        kwargs["settings"] = connection["settings"]
    if connection.get("connect_timeout"):
        kwargs["connect_timeout"] = int(connection["connect_timeout"])
    return clickhouse_connect.get_client(**kwargs)


def _base_query(resource_config: Dict[str, Any]) -> str:
    query = resource_config.get("query")
    if query:
        return query.strip().rstrip(";")
    table = resource_config.get("table")
    if not table:
        raise ValueError(
            f"resource '{resource_config.get('name')}' declares neither 'query' nor 'table'"
        )
    columns = resource_config.get("columns") or "*"
    if isinstance(columns, (list, tuple)):
        columns = ", ".join(columns)
    where = resource_config.get("where")
    sql = f"SELECT {columns} FROM {table}"
    if where:
        sql = f"{sql} WHERE {where}"
    return sql


def _incremental_query(
    base: str,
    cursor_column: Optional[str],
    last_value: Any,
    lookback_seconds: int,
    limit: int,
) -> str:
    """Wrap the base query with a cursor filter and deterministic ordering.

    Wrapping in a subquery (rather than splicing a WHERE clause into the
    user's SQL) keeps arbitrary queries — GROUP BY, JOIN, whatever —
    working untouched.
    """
    sql = f"SELECT * FROM ({base}) AS _src"
    if cursor_column and last_value is not None:
        # Telemetry arrives out of order: a lookback re-reads a trailing
        # window so late spans are not lost. Rows already loaded are
        # deduplicated downstream by the resource primary key.
        if lookback_seconds:
            sql += (
                f" WHERE _src.{cursor_column} > "
                f"(toDateTime64(%(cursor)s, 3) - INTERVAL {int(lookback_seconds)} SECOND)"
            )
        else:
            sql += f" WHERE _src.{cursor_column} > %(cursor)s"
    if cursor_column:
        sql += f" ORDER BY _src.{cursor_column}"
    if limit:
        sql += f" LIMIT {int(limit)}"
    return sql


def _rows_from_block(column_names: List[str], block: Any) -> Iterator[Dict[str, Any]]:
    for row in block:
        yield dict(zip(column_names, row))


def clickhouse_query(
    connection: Dict[str, Any],
    resources: List[Dict[str, Any]],
    chunk_size: int = 50_000,
):
    """Build a dlt source over one or more ClickHouse queries.

    Each entry in ``resources`` is a dict::

        {
          "name": "ci_spans",              # dlt resource / destination table
          "query": "SELECT * FROM otel_traces WHERE SpanName = 'suite.run'",
          # or: "table": "otel_traces", "where": "...", "columns": [...]
          "cursor_column": "Timestamp",    # optional incremental cursor
          "lookback_seconds": 300,         # re-read window for late arrivals
          "primary_key": ["TraceId", "SpanId"],
          "limit": 0,
        }

    Incremental position is held in dlt's *resource* state (not source
    state) so multiple resources in one source advance independently.
    """

    @dlt.source(name="clickhouse_query")
    def _source():
        for resource_config in resources:
            name = resource_config.get("name")
            if not name:
                raise ValueError("every clickhouse_query resource needs a 'name'")

            def _make_generator(config: Dict[str, Any]):
                def _generate():
                    cursor_column = config.get("cursor_column")
                    lookback = int(config.get("lookback_seconds") or 0)
                    limit = int(config.get("limit") or 0)
                    state = dlt.current.resource_state()
                    last_value = state.get("last_cursor_value") if cursor_column else None

                    sql = _incremental_query(
                        _base_query(config), cursor_column, last_value, lookback, limit
                    )
                    parameters = {"cursor": last_value} if (cursor_column and last_value is not None) else None

                    client = _client(connection)
                    try:
                        logger.info("clickhouse_query[%s]: %s", config["name"], sql)
                        stream = client.query_row_block_stream(sql, parameters=parameters)
                        with stream:
                            column_names = stream.source.column_names
                            highest = last_value
                            for block in stream:
                                batch: List[Dict[str, Any]] = []
                                for row in _rows_from_block(list(column_names), block):
                                    if cursor_column:
                                        value = row.get(cursor_column)
                                        if value is not None and (highest is None or value > highest):
                                            highest = value
                                    batch.append(row)
                                    if len(batch) >= chunk_size:
                                        yield batch
                                        batch = []
                                if batch:
                                    yield batch
                            # Advance only after the stream drained cleanly;
                            # a mid-stream failure must not skip rows.
                            if cursor_column and highest is not None:
                                state["last_cursor_value"] = highest
                    finally:
                        client.close()

                return _generate

            hints: Dict[str, Any] = {}
            if resource_config.get("primary_key"):
                hints["primary_key"] = resource_config["primary_key"]
            if resource_config.get("write_disposition"):
                hints["write_disposition"] = resource_config["write_disposition"]
            elif resource_config.get("primary_key"):
                hints["write_disposition"] = "merge"

            # Pin the attribute maps to JSON so the staging load preserves
            # them instead of exploding them into flattened, lower-cased
            # columns. Pass an explicit (possibly empty) `map_columns` to
            # override.
            map_columns = resource_config.get("map_columns")
            if map_columns is None:
                map_columns = DEFAULT_MAP_COLUMNS
            if map_columns:
                hints["columns"] = {
                    column: {"data_type": "json"} for column in map_columns
                }

            yield dlt.resource(
                _make_generator(dict(resource_config, name=name)),
                name=name,
                **hints,
            )

    return _source()

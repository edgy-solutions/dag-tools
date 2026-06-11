"""Shared Dagster asset introspection contract used by:
  * The Domain Broker — for runtime IO manager classification / mesh routing.
  * The Dagster qualification survey (`dagtools survey`) — for per-build
    inventory published to the MinIO registry.

See the individual modules for design notes:
  * `schema`     — the versioned ``AssetRecord`` data contract.
  * `classifier` — FQN -> family mapping with MRO walking and substring fallback.
  * `extractors` — version-gated, soft-failing walk of a Dagster ``Definitions``.

Adding new fields to ``AssetRecord`` is the normal flow; bump
``SCHEMA_VERSION`` in the same commit. See ``schema.py`` for evolution rules.
"""
from .classifier import (
    FAMILY_REGISTRY,
    FAMILY_CLICKHOUSE,
    FAMILY_DUCKDB,
    FAMILY_FILESYSTEM,
    FAMILY_IN_MEMORY,
    FAMILY_POSTGRES,
    FAMILY_S3_DELTA,
    FAMILY_S3_ICEBERG,
    FAMILY_S3_PARQUET,
    FAMILY_SNOWFLAKE,
    classify,
    fqn,
)
from .extractors import extract_records
from .schema import SCHEMA_VERSION, AssetRecord

__all__ = [
    "SCHEMA_VERSION",
    "AssetRecord",
    "classify",
    "extract_records",
    "fqn",
    "FAMILY_REGISTRY",
    "FAMILY_POSTGRES",
    "FAMILY_CLICKHOUSE",
    "FAMILY_SNOWFLAKE",
    "FAMILY_DUCKDB",
    "FAMILY_S3_PARQUET",
    "FAMILY_S3_ICEBERG",
    "FAMILY_S3_DELTA",
    "FAMILY_FILESYSTEM",
    "FAMILY_IN_MEMORY",
]

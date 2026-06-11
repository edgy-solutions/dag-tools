"""Maps fully-qualified IO manager class names to coarse `family` labels.

The Domain Broker historically classified IO managers by substring-matching
the lowercased class name (`"postgres" in class_name.lower()` etc.). That
silently misclassifies custom forks (a ``PostgresVectorIOManager`` fork that
drops "postgres" from its name, or a generic ``VectorStorageManager`` that
happens to contain "store"). The new classifier replaces it with:

  1. An explicit FQN -> family registry (the strongest signal).
  2. MRO walking — a class whose own FQN isn't registered falls back to its
     ancestors, catching custom subclasses of stock IO managers without
     extra config.
  3. A substring-matching last resort, logged at WARNING so unknown
     classes get noticed and added to the registry over time.

Add entries to ``FAMILY_REGISTRY`` when introducing or discovering new IO
manager classes. The registry is meant to grow — that's the point.

Family labels are stable strings persisted in `AssetRecord.io_manager_family`
and consumed by the Broker for routing. **Do not rename existing labels** —
that would silently break routing for any inventory written before the rename.
"""
from __future__ import annotations

import logging
from typing import Optional, Union


logger = logging.getLogger(__name__)


# --- family labels ----------------------------------------------------------
# Stable strings; do not rename.
FAMILY_POSTGRES = "postgres"
FAMILY_CLICKHOUSE = "clickhouse"
FAMILY_SNOWFLAKE = "snowflake"
FAMILY_DUCKDB = "duckdb"
FAMILY_S3_PARQUET = "s3_parquet"
FAMILY_S3_ICEBERG = "s3_iceberg"
FAMILY_S3_DELTA = "s3_delta"
FAMILY_FILESYSTEM = "filesystem"
FAMILY_IN_MEMORY = "in_memory"


# --- explicit FQN registry --------------------------------------------------
# Exact-match wins (strongest signal). Add new IO manager classes here when
# they appear in the fleet. MRO walking catches subclasses for free.
FAMILY_REGISTRY: dict[str, str] = {
    # dagster built-ins
    "dagster._core.storage.mem_io_manager.InMemoryIOManager": FAMILY_IN_MEMORY,
    "dagster._core.storage.fs_io_manager.PickledObjectFilesystemIOManager": FAMILY_FILESYSTEM,
    "dagster._core.storage.fs_io_manager.FilesystemIOManager": FAMILY_FILESYSTEM,

    # dag-tools native
    "dag_tools.io_managers.arrow.ArrowIOManager": FAMILY_S3_PARQUET,
    "dag_tools.io_managers.arrow.ConfigurableArrowIOManager": FAMILY_S3_PARQUET,
    "dag_tools.io_managers.s3.FileObjectS3IOManager": FAMILY_S3_PARQUET,
    "dag_tools.io_managers.s3.S3FileIOManager": FAMILY_S3_PARQUET,
    "dag_tools.io_managers.cortex_io_manager.CortexPolarsIOManager": FAMILY_S3_PARQUET,

    # common dagster-* extras (best-effort; subclasses caught via MRO walking)
    "dagster_snowflake_pandas.snowflake_pandas_type_handler.SnowflakePandasIOManager": FAMILY_SNOWFLAKE,
    "dagster_duckdb.io_manager.DuckDBIOManager": FAMILY_DUCKDB,
}


# --- substring fallback -----------------------------------------------------
# Last-resort; logged at WARNING so unknown IO managers surface.
# Order matters: more specific substrings come first.
_SUBSTRING_FALLBACKS: list[tuple[str, str]] = [
    ("iceberg", FAMILY_S3_ICEBERG),
    ("delta", FAMILY_S3_DELTA),
    ("clickhouse", FAMILY_CLICKHOUSE),
    ("postgres", FAMILY_POSTGRES),
    ("snowflake", FAMILY_SNOWFLAKE),
    ("duckdb", FAMILY_DUCKDB),
    ("parquet", FAMILY_S3_PARQUET),
    ("s3", FAMILY_S3_PARQUET),
    ("filesystem", FAMILY_FILESYSTEM),
    ("memory", FAMILY_IN_MEMORY),
]


def fqn(cls: type) -> str:
    """Return the fully-qualified class name for a class object."""
    return f"{cls.__module__}.{cls.__qualname__}"


def classify(
    target: Union[type, str, None],
    allow_substring_fallback: bool = True,
) -> Optional[str]:
    """Classify an IO manager class into a coarse family label.

    Resolution order:

      1. The class's own FQN, looked up in :data:`FAMILY_REGISTRY`.
      2. Walk the MRO; first ancestor FQN that matches wins. Catches
         custom subclasses of stock IO managers transparently.
      3. (Optional) substring-match on the lowercased FQN. Logged at
         WARNING so unknown classes get noticed and added to the registry.

    Args:
      target: A class object (preferred — enables MRO walking) or a
        string FQN (registry + substring only — no MRO).
      allow_substring_fallback: When False, skip step 3 entirely and
        return None instead. Useful for tests that want strict behavior.

    Returns:
      The family label string, or None if nothing matched.
    """
    if target is None:
        return None

    if isinstance(target, str):
        if target in FAMILY_REGISTRY:
            return FAMILY_REGISTRY[target]
        if allow_substring_fallback:
            return _substring_classify(target)
        return None

    # Class object: try own FQN, then walk MRO.
    own_fqn = fqn(target)
    if own_fqn in FAMILY_REGISTRY:
        return FAMILY_REGISTRY[own_fqn]

    for base in target.__mro__[1:]:  # skip self
        base_fqn = fqn(base)
        if base_fqn in FAMILY_REGISTRY:
            family = FAMILY_REGISTRY[base_fqn]
            logger.debug(
                "classify: %s matched ancestor %s -> %s",
                own_fqn, base_fqn, family,
            )
            return family

    if allow_substring_fallback:
        return _substring_classify(own_fqn)
    return None


def _substring_classify(fqn_str: str) -> Optional[str]:
    """Last-resort substring match on the lowercased FQN.

    Logged at WARNING so unknown IO managers surface in logs and get added
    to FAMILY_REGISTRY explicitly. This is a code smell — every WARNING here
    is either a missing registry entry or genuinely unfamiliar territory.
    """
    lowered = fqn_str.lower()
    for substring, family in _SUBSTRING_FALLBACKS:
        if substring in lowered:
            logger.warning(
                "classify: substring fallback for %s -> %s "
                "(add an explicit FAMILY_REGISTRY entry to suppress this warning)",
                fqn_str, family,
            )
            return family
    logger.warning("classify: no match for %s; returning None", fqn_str)
    return None

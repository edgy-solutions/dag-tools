"""Dagster IO managers shipped by dag-tools.

Each submodule has its own heavy third-party deps that aren't required
by every consumer:

  * ``s3.py``        -> dagster-aws
  * ``arrow.py``     -> pyarrow + fsspec + s3fs
  * ``cortex_io_manager.py`` -> the cortex data client (httpx + polars
                                + driver-of-the-day for each backend)
  * ``sql.py``       -> connectorx + SQLAlchemy
  * ``delta.py``     -> deltalake-rs
  * ``duckdb.py``    -> duckdb

To avoid pulling every heavy dep into a consumer that only wants one
IO manager, this package uses PEP 562 lazy attribute loading. The
top-level names below only trigger their submodule import when the
attribute is actually accessed — so e.g.

    from dag_tools.io_managers import CortexPolarsIOManager

imports ``cortex_io_manager.py`` only, leaving ``s3.py`` /
``dagster_aws`` un-imported.

Direct submodule imports also continue to work without dragging
sibling submodules in:

    from dag_tools.io_managers.cortex_io_manager import CortexPolarsIOManager
    from dag_tools.io_managers.sql import SQLIOManager
"""

# Public name → (submodule, attribute_in_submodule). The lazy loader
# below reads this table to resolve attribute access. Adding a new
# IO manager: add an entry here, and ``from dag_tools.io_managers
# import NewIOManager`` works without changing anything else.
_LAZY_EXPORTS = {
    "S3FileIOManager": ("s3", "S3FileIOManager"),
    "pandas_to_excel_stream": ("s3", "pandas_to_excel_stream"),
    "ConfigurableArrowIOManager": ("arrow", "ConfigurableArrowIOManager"),
    "CortexPolarsIOManager": ("cortex_io_manager", "CortexPolarsIOManager"),
    "SQLConfig": ("sql", "SQLConfig"),
    "SQLIOManager": ("sql", "SQLIOManager"),
    "ConfigurableSQLIOManager": ("sql", "ConfigurableSQLIOManager"),
    "get_source_fn_default": ("sql", "get_source_fn_default"),
    "set_source_fn_for_asset_class": ("sql", "set_source_fn_for_asset_class"),
    "DeltaIOManager": ("delta", "DeltaIOManager"),
    "ConfigurableDeltaIOManager": ("delta", "ConfigurableDeltaIOManager"),
    "DuckDBIOManager": ("duckdb", "DuckDBIOManager"),
    "ConfigurableDuckDBIOManager": ("duckdb", "ConfigurableDuckDBIOManager"),
    # The mesh-publishing protocol, for IO managers that are NOT dag-tools'.
    # The protocol is duck-typed — the broker only checks for
    # physical_coordinates() — but until this existed it was documented solely
    # by four independent implementations, so a third party had to copy one and
    # guess which parts generalised. See mesh_publishing.py.
    "MeshPublishable": ("mesh_publishing", "MeshPublishable"),
    "KNOWN_SOURCE_TYPES": ("mesh_publishing", "KNOWN_SOURCE_TYPES"),
}

__all__ = sorted(_LAZY_EXPORTS.keys())


def __getattr__(name: str):
    """Lazy attribute loader (PEP 562).

    Called by Python when a name is accessed on this module that
    wasn't found in the module's namespace. Resolves the name via the
    ``_LAZY_EXPORTS`` table, imports the corresponding submodule
    on demand, and returns the attribute. Caches happen at the
    Python import level — the second access of the same name skips
    this function and reads the value out of ``sys.modules``.
    """
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(
            f"module 'dag_tools.io_managers' has no attribute {name!r}"
        )
    submodule_name, attr_name = target
    from importlib import import_module

    module = import_module(f"dag_tools.io_managers.{submodule_name}")
    return getattr(module, attr_name)


def __dir__():
    """Make ``dir(dag_tools.io_managers)`` and tab-completion list the
    lazy exports, even though they haven't been loaded yet."""
    return list(__all__)

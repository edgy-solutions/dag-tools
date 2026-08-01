"""Column schema for materialization metadata, shared by the IO managers.

Every producing IO manager should publish the columns of what it wrote.
The Dagster UI renders them, and the DataHub catalog sensor turns them
into a ``schemaMetadata`` aspect — without it a dataset registers with
properties and lineage but no columns.

That used to reach the catalog through ``CortexPolarsIOManager``, which
emitted to DataHub directly. When that emit was removed (an IO manager is
bound per-asset and may be bound to assets another deployment owns, so it
cannot honestly claim authorship) nothing replaced it, and the aspect
silently stopped being written.

Lives here rather than in each manager because the extraction is the same
everywhere and the failure is invisible: a manager that quietly skipped
this would look identical to one that worked, right up until someone
opened the catalog.
"""
from typing import Any, List, Optional, Sequence, Tuple

from dagster import MetadataValue

# The conventional key. dagster-dbt attaches column schema under this
# name, so using it means dbt assets and ours are handled by one path.
COLUMN_SCHEMA_KEY = "dagster/column_schema"


def _columns_of(obj: Any) -> Optional[List[Tuple[str, str]]]:
    """``(name, type)`` pairs from whatever the asset returned.

    Reads DECLARED schema only — nothing is materialized, collected, or
    scanned. That is essential for a ``pyarrow.RecordBatchReader``, which
    is a one-shot stream: consuming it to inspect columns would leave
    nothing for the writer.
    """
    if isinstance(obj, dict):
        # Multi-output writes store each value separately, but the
        # metadata slot is singular, so describe the first.
        obj = next(iter(obj.values()), None)
    if obj is None:
        return None

    try:
        import polars as pl

        if isinstance(obj, pl.LazyFrame):
            s = obj.collect_schema()
            return list(zip(s.names(), (str(t) for t in s.dtypes())))
        if isinstance(obj, pl.DataFrame):
            return [(n, str(t)) for n, t in zip(obj.columns, obj.dtypes)]
    except ImportError:
        pass

    try:
        import pandas as pd

        if isinstance(obj, pd.DataFrame):
            return [(str(n), str(t)) for n, t in obj.dtypes.items()]
    except ImportError:
        pass

    # DuckDB relation: a lazy query that already knows its output shape.
    if hasattr(obj, "columns") and hasattr(obj, "types"):
        try:
            names = list(obj.columns)
            if names:
                return list(zip(names, (str(t) for t in obj.types)))
        except Exception:
            pass

    # pyarrow Table / RecordBatchReader / Dataset, and anything else
    # exposing an Arrow-shaped schema.
    schema = getattr(obj, "schema", None)
    if schema is not None and hasattr(schema, "names"):
        try:
            return [(n, str(schema.field(n).type)) for n in schema.names]
        except Exception:
            return None

    return None


def column_schema_metadata(obj: Any) -> Optional[MetadataValue]:
    """``TableSchemaMetadataValue`` for ``obj``, or None if undeterminable.

    Best-effort by contract: a missing schema costs one metadata field,
    and must never fail a materialization whose data is already written.
    """
    from dagster import TableColumn, TableSchema

    try:
        columns = _columns_of(obj)
        if not columns:
            return None
        return MetadataValue.table_schema(
            TableSchema(columns=[TableColumn(name=n, type=t) for n, t in columns])
        )
    except Exception:
        return None


def add_column_schema(metadata: dict, obj: Any) -> dict:
    """Insert the column schema into ``metadata`` when one is derivable."""
    schema = column_schema_metadata(obj)
    if schema is not None:
        metadata[COLUMN_SCHEMA_KEY] = schema
    return metadata

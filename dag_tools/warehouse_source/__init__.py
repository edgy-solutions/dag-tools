"""Warehouse source: a labelled source dataset, observed rather than moved.

Step 4a of the classification build (ADR-0064). One component instance
describes one source dataset: where it physically lives, which columns carry
which classification labels, who owns it and where the labels came from. It
moves no data -- it observes (row count, schema hash, as-of) and publishes
what the catalog needs to describe the object a broker authorizes reads of.

Everything in this package is **pure**: documents and rows in, plans and
payloads out. No Dagster, no DataHub, no IO. The component next door
(`dag_tools.components.warehouse_source`) is the only thing that reads the
mounted dimensions file, talks to a warehouse or emits to a catalog -- which
is what makes the labelling rules in `AGENTS.md` §4l testable without a
cluster, a warehouse or a ConfigMap.

**Nothing here names a database, schema, table, column, dimension or value**,
not even as a default. The dimension that orders the others is discovered
from the mounted document's `hierarchical` flag; see `dimensions.py`.
"""
from .catalog import (
    CLASSIFICATION_ORIGIN_PROPERTY,
    CLASSIFICATION_PROPERTY,
    CatalogPayload,
    ColumnPublication,
    build_catalog_payload,
)
from .dimensions import (
    DEFAULT_DIMENSIONS_PATH,
    DIMENSIONS_ENV_VAR,
    DOCUMENT_KEY,
    Dimension,
    DimensionSet,
    DimensionsError,
    DimensionValue,
    Label,
    resolve_dimensions_path,
)
from .observe import (
    DEFAULT_COLUMNS_STATEMENT,
    DEFAULT_ROW_COUNT_STATEMENT,
    Observation,
    ObservationPlan,
    ObservationError,
    observe,
    plan_observation,
    read_as_of,
    read_column_names,
    read_row_count,
    schema_hash,
)
from .spec import (
    ACCESS_MODES,
    LABEL_ORIGINS,
    ColumnLabel,
    LabelledSource,
    ResolvedLabels,
    SourceSpec,
    SourceSpecError,
    env_of,
    label_source,
    load_source_spec,
    platform_of,
)

__all__ = [
    "ACCESS_MODES",
    "CLASSIFICATION_ORIGIN_PROPERTY",
    "CLASSIFICATION_PROPERTY",
    "CatalogPayload",
    "ColumnLabel",
    "ColumnPublication",
    "DEFAULT_COLUMNS_STATEMENT",
    "DEFAULT_DIMENSIONS_PATH",
    "DEFAULT_ROW_COUNT_STATEMENT",
    "DIMENSIONS_ENV_VAR",
    "DOCUMENT_KEY",
    "Dimension",
    "DimensionSet",
    "DimensionValue",
    "DimensionsError",
    "LABEL_ORIGINS",
    "Label",
    "LabelledSource",
    "Observation",
    "ObservationError",
    "ObservationPlan",
    "ResolvedLabels",
    "SourceSpec",
    "SourceSpecError",
    "build_catalog_payload",
    "env_of",
    "label_source",
    "load_source_spec",
    "observe",
    "plan_observation",
    "platform_of",
    "read_as_of",
    "read_column_names",
    "read_row_count",
    "resolve_dimensions_path",
    "schema_hash",
]

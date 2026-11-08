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
    DIMENSION_TYPES,
    DIMENSIONS_ENV_VAR,
    DOCUMENT_KEY,
    EXCLUSIONARY,
    HIERARCHICAL,
    PERMISSIVE_SET,
    RESTRICTIVE_SET,
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
from .propagate import (
    BASES,
    COLUMN_LINEAGE,
    TABLE_MAX,
    AppliedDowngrade,
    ColumnDecision,
    ColumnRef,
    DowngradeRule,
    Propagation,
    PropagationError,
    compose,
    propagate,
)
from .spec import (
    ACCESS_MODES,
    LABEL_ORIGINS,
    REFUSED_PLATFORM,
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
    "AppliedDowngrade",
    "BASES",
    "build_catalog_payload",
    "CatalogPayload",
    "CLASSIFICATION_ORIGIN_PROPERTY",
    "CLASSIFICATION_PROPERTY",
    "COLUMN_LINEAGE",
    "ColumnDecision",
    "ColumnLabel",
    "ColumnPublication",
    "ColumnRef",
    "compose",
    "DEFAULT_COLUMNS_STATEMENT",
    "DEFAULT_DIMENSIONS_PATH",
    "DEFAULT_ROW_COUNT_STATEMENT",
    "Dimension",
    "DIMENSION_TYPES",
    "DIMENSIONS_ENV_VAR",
    "DimensionsError",
    "DimensionSet",
    "DimensionValue",
    "DOCUMENT_KEY",
    "DowngradeRule",
    "env_of",
    "EXCLUSIONARY",
    "HIERARCHICAL",
    "Label",
    "LABEL_ORIGINS",
    "label_source",
    "LabelledSource",
    "load_source_spec",
    "Observation",
    "ObservationError",
    "ObservationPlan",
    "observe",
    "PERMISSIVE_SET",
    "plan_observation",
    "platform_of",
    "REFUSED_PLATFORM",
    "propagate",
    "Propagation",
    "PropagationError",
    "read_as_of",
    "read_column_names",
    "read_row_count",
    "resolve_dimensions_path",
    "ResolvedLabels",
    "RESTRICTIVE_SET",
    "schema_hash",
    "SourceSpec",
    "SourceSpecError",
    "TABLE_MAX",
]

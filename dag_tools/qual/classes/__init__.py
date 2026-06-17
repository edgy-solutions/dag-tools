"""Phase 2 Q1 — fleet equivalence-class matrix.

Reads the qualification manifest's pinned inventories, computes class
keys per the recipe, picks representatives with runnability labels,
and publishes ``classes/equivalence_classes.json`` + a human-readable
``classes/equivalence_classes.md`` companion to the registry.

Public surface:
  * :func:`build_class_matrix` — read manifest, fetch pinned inventories,
    return the populated :class:`ClassMatrix`.
  * :func:`publish_class_matrix` — write to registry (immutable per qual_id).
  * :func:`render_markdown` — operator-eyeballing companion.
  * Data shapes: :class:`Runnability`, :class:`ClassKeyComponents`,
    :class:`ClassMember`, :class:`Representative`, :class:`EquivalenceClass`,
    :class:`ClassMatrix`.
"""
from .builder import (
    build_class_matrix,
    publish_class_matrix,
    render_markdown,
)
from .key import (
    SCHEMA_VERSION_CLASS_MATRIX,
    ClassKeyComponents,
    ClassMatrix,
    ClassMember,
    EquivalenceClass,
    Representative,
    Runnability,
    class_hash,
    compute_class_key,
)
from .selection import classify_runnability, pick_representatives

__all__ = [
    "SCHEMA_VERSION_CLASS_MATRIX",
    # data shapes
    "ClassKeyComponents",
    "ClassMatrix",
    "ClassMember",
    "EquivalenceClass",
    "Representative",
    "Runnability",
    # key
    "class_hash",
    "compute_class_key",
    # selection
    "pick_representatives",
    "classify_runnability",
    # builder
    "build_class_matrix",
    "publish_class_matrix",
    "render_markdown",
]

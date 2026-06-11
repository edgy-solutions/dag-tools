"""Object-store layout, client, and staleness reporter for the qualification
registry.

Public surface:
  * :class:`StorageSettings` and :class:`S3Storage` — low-level S3 / MinIO
    plumbing with explicit immutable vs. mutable put modes.
  * :class:`InventoryRegistry` — the high-level facade that the survey
    publishes through and the qualification CLI reads from.
  * :class:`BuildMeta` and :class:`LatestPointer` — the pydantic shapes
    that travel through ``meta.json`` and ``latest.json``.
  * :class:`StalenessState`, :class:`RepoStatus`, :class:`StatusReport`,
    :func:`compute_staleness` — the ``dagtools registry status`` core.
  * :mod:`.layout` — every object-store key lives here.

See :mod:`.layout` for the canonical bucket layout, and :mod:`.client` for
the immutability / write-last semantics.
"""
from . import layout
from .client import (
    BuildMeta,
    ImmutableKeyExists,
    InventoryRegistry,
    LatestPointer,
    S3Storage,
    StorageSettings,
)
from .status import (
    RepoStatus,
    StalenessState,
    StatusReport,
    compute_staleness,
)

__all__ = [
    "layout",
    "BuildMeta",
    "ImmutableKeyExists",
    "InventoryRegistry",
    "LatestPointer",
    "S3Storage",
    "StorageSettings",
    "RepoStatus",
    "StalenessState",
    "StatusReport",
    "compute_staleness",
]

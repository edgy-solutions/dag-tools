"""Phase 2 qualification orchestration.

Public surface for Phase Q0 (manifest creation):
  * :func:`create_qualification` — the orchestrator wired into ``dagtools qual init``.
  * :class:`QualificationManifest` and its constituent pydantic models.
  * :func:`compute_co_upgrade_risks` — diffs baseline vs. candidate pin sets.

Phases Q1..Q6 land in subsequent commits per ``docs/RECIPE.md``.
"""
from .init import create_qualification, default_local_manifest_path
from .manifest import (
    SCHEMA_VERSION,
    CoUpgradeRisk,
    Deployment,
    InventoryPin,
    QualificationManifest,
    Selection,
    VersionTarget,
)
from .risks import compute_co_upgrade_risks

__all__ = [
    "SCHEMA_VERSION",
    "CoUpgradeRisk",
    "Deployment",
    "InventoryPin",
    "QualificationManifest",
    "Selection",
    "VersionTarget",
    "compute_co_upgrade_risks",
    "create_qualification",
    "default_local_manifest_path",
]

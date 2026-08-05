"""Equivalence-class key definition + deterministic hashing.

Recipe rule (Phase Q1, item 2):

  > Class key:
  >   (compute_kind, io_manager_class, partitions_def_class,
  >    frozenset(partition_mapping_classes), frozenset(resource_classes),
  >    frozenset(integration_libs), has_asset_checks,
  >    automation_condition_type)

Plus the "custom dbt translator always forms its own class" rule (item 3) —
which we implement by appending the FQN of any custom DagsterDbtTranslator
subclasses in the asset's repo to the key, but ONLY for assets that
actually use ``dagster_dbt``.

The exploded :class:`ClassKeyComponents` is what we persist (so operators
can read the structure of a class). The :func:`class_hash` is a 12-char
SHA-256 prefix over the same components in a deterministic JSON form —
two assets with the same components land in the same class.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from dag_tools.inventory import AssetRecord


SCHEMA_VERSION_CLASS_MATRIX = 1


# ---------------------------------------------------------------------------
# Runnability — recipe Phase Q1 item 5.
# ---------------------------------------------------------------------------


class Runnability(str, Enum):
    """How Phase Q2/Q4 can exercise a representative.

    * ``RUNNABLE`` — resources redirect cleanly to staging targets via the
      override layer; can be launched on the test deployment as-is.
    * ``SYNTHETIC_REQUIRED`` — touches prod-only/expensive systems
      (warehouse-only schemas, hosted SaaS, paid APIs); needs a synthetic
      probe generated under the ``dag-tools-probes`` location.
    * ``OBSERVE_ONLY`` — external/observable assets that aren't run by
      Dagster; checked via metadata diff rather than materialization.
    """

    RUNNABLE = "runnable"
    SYNTHETIC_REQUIRED = "synthetic_required"
    OBSERVE_ONLY = "observe_only"


# ---------------------------------------------------------------------------
# Class key components — exploded for downstream readability.
# ---------------------------------------------------------------------------


class ClassKeyComponents(BaseModel):
    """The eight (+1) key components per the recipe, in a JSON-friendly shape.

    The hash is computed from this object in a deterministic JSON form
    (sorted keys, frozensets rendered as sorted lists). Two assets with
    identical components produce identical hashes regardless of original
    field ordering on the AssetRecord.
    """
    model_config = ConfigDict(extra="ignore")

    compute_kind: Optional[str] = None
    io_manager_class: Optional[str] = None
    partitions_def_class: Optional[str] = None
    partition_mapping_classes: List[str] = Field(default_factory=list)
    resource_classes: List[str] = Field(default_factory=list)
    """Non-IO-manager resource class FQNs, sorted. The io_manager class
    appears as its own component above; including it here too would just
    add noise without changing class distinctions."""
    integration_libs: List[str] = Field(default_factory=list)
    has_asset_checks: bool = False
    automation_condition_type: Optional[str] = None

    custom_dbt_translator_classes: List[str] = Field(default_factory=list)
    """FQNs of custom ``DagsterDbtTranslator`` subclasses in this asset's
    repo. Only populated for assets that use ``dagster_dbt`` (per
    ``integration_libs``); empty otherwise. Custom translators always
    form their own classes — that's the point."""


# ---------------------------------------------------------------------------
# Per-asset + per-class data shapes for the published artifact.
# ---------------------------------------------------------------------------


class ClassMember(BaseModel):
    """One asset's identity inside an equivalence class."""
    model_config = ConfigDict(extra="ignore")

    repo: str
    git_sha: str
    asset_key: List[str]
    location: Optional[str] = None
    tags: Dict[str, str] = Field(default_factory=dict)
    is_executable: Optional[bool] = None
    """From ``AssetRecord.is_executable``. False means Dagster cannot
    materialize this asset at all, which forces OBSERVE_ONLY regardless of
    tags. None means the survey predates the field — treated as unknown,
    so old inventories classify exactly as they did before."""


class Representative(BaseModel):
    """A selected representative for the class, annotated with runnability."""
    model_config = ConfigDict(extra="ignore")

    repo: str
    git_sha: str
    asset_key: List[str]
    runnability: Runnability
    runnability_reason: str
    is_preferred: bool = False
    """True when this rep matched the configured ``prefer_tag`` from the manifest."""


class EquivalenceClass(BaseModel):
    """One equivalence class — members + chosen representatives."""
    model_config = ConfigDict(extra="ignore")

    class_hash: str
    key: ClassKeyComponents
    member_count: int
    member_repo_count: int
    """How many distinct repos this class spans. >=2 is healthier coverage
    for the regression — a class that lives in only one repo can't tell
    you whether a candidate change affects the fleet broadly."""
    members: List[ClassMember]
    representatives: List[Representative]


class ClassMatrix(BaseModel):
    """The full Q1 artifact persisted to ``classes/equivalence_classes.json``."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION_CLASS_MATRIX)
    qual_id: str
    generated_at: datetime
    asset_count: int
    class_count: int
    coverage_by_runnability: Dict[str, int] = Field(default_factory=dict)
    """Count of *representatives* (not members) per runnability bucket.
    Sums to ``sum(reps_per_class for each class)`` — usually less than
    the asset count because each class contributes ``reps_per_class``
    representatives, not all members."""
    classes: List[EquivalenceClass]


# ---------------------------------------------------------------------------
# Key construction + hashing
# ---------------------------------------------------------------------------


def compute_class_key(
    record: AssetRecord,
    *,
    custom_dbt_translators_in_repo: List[str] | None = None,
) -> ClassKeyComponents:
    """Build the class key components for one ``AssetRecord``.

    Args:
      record: the inventory record.
      custom_dbt_translators_in_repo: list of custom
        ``DagsterDbtTranslator`` subclass FQNs detected in the asset's
        repo (from ``dbt_projects.json``). When the asset uses
        ``dagster_dbt`` (per ``integration_libs``), these FQNs become
        part of its class key so it lands in its own class.
    """
    io_key = record.io_manager_key
    # Exclude the IO manager's own class from the resource_classes set —
    # it's already represented as its own key component, and
    # double-counting just adds noise.
    non_io_resource_classes = sorted({
        fqn for key, fqn in record.resource_classes.items()
        if key != io_key
    })

    integration_libs = sorted(set(record.integration_libs))

    custom_translators: List[str] = []
    if custom_dbt_translators_in_repo and _uses_dagster_dbt(record):
        custom_translators = sorted(set(custom_dbt_translators_in_repo))

    return ClassKeyComponents(
        compute_kind=record.compute_kind,
        io_manager_class=record.io_manager_class,
        partitions_def_class=record.partitions_def_class,
        partition_mapping_classes=sorted(set(record.partition_mapping_classes)),
        resource_classes=non_io_resource_classes,
        integration_libs=integration_libs,
        has_asset_checks=bool(record.has_asset_checks),
        automation_condition_type=record.automation_condition_type,
        custom_dbt_translator_classes=custom_translators,
    )


def class_hash(key: ClassKeyComponents) -> str:
    """Deterministic 12-char hash over the key components.

    Uses SHA-256 with ``json.dumps(..., sort_keys=True)`` so the same
    components always produce the same hash, regardless of how the
    pydantic model was constructed.
    """
    payload = json.dumps(key.model_dump(), sort_keys=True, default=str)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return digest[:12]


def _uses_dagster_dbt(record: AssetRecord) -> bool:
    """Heuristic: an asset uses dbt when its inventory mentions
    ``dagster_dbt`` as an integration lib, or its compute_kind starts with
    ``dbt``. We OR the two so neither convention misses."""
    if "dagster_dbt" in (record.integration_libs or []):
        return True
    ck = (record.compute_kind or "").lower()
    return ck.startswith("dbt")

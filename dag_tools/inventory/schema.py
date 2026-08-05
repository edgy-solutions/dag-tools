"""Versioned, additive-only data contract for Dagster asset introspection.

Both the runtime data-mesh Domain Broker and the Dagster qualification survey
(`dagtools survey`) import `AssetRecord` from this module. Cross-process
compatibility is the whole point: long-lived Broker pods will be reading
records written by newer CI surveys, and vice versa. Two rules keep that
working:

  1. `SCHEMA_VERSION` is monotonic and bumps on every additive change.
  2. Evolution is additive-only — never rename or remove a field, only add
     new ones with `None` or default values. Older readers tolerate unknown
     fields via `extra="ignore"`, so newer writers stay readable.

If a field truly needs to be removed or renamed, that's a major version bump
of the shared library and requires coordinated rollout — talk to the runtime
mesh team first.

When adding a field:

  * Use ``Optional[...]`` with a default of ``None`` (or a typed empty
    default like ``default_factory=list``).
  * Bump ``SCHEMA_VERSION`` by 1 in the same commit.
  * Update the docstring in `dag_tools.inventory.__init__` if the field
    changes the public surface.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


SCHEMA_VERSION = 2
"""Monotonic schema version for AssetRecord. Bump on every additive change.

History:
  1 — initial schema (asset identity, IO manager, partitioning, resources,
      automation, freshness/backfill, tags, checks, jobs, URN sidecar).
  2 — is_executable, so qualification can tell an asset Dagster runs from
      an external/source asset it only observes.
"""


class AssetRecord(BaseModel):
    """A single Dagster asset's structural metadata, version-tagged for
    cross-process compatibility.

    See module docstring for evolution rules. Required fields are limited
    to ``asset_key`` and ``schema_version`` (which defaults). Every other
    field is optional / has a default — soft per-field extraction failure
    leaves a field as ``None`` rather than aborting the whole record.
    """

    model_config = ConfigDict(
        # Tolerate unknown fields so older readers can ingest newer writers.
        extra="ignore",
    )

    # --- contract version ----------------------------------------------------
    schema_version: int = Field(
        default=SCHEMA_VERSION,
        description="Schema version of this record. Monotonic; bump on every additive change.",
    )

    # --- identity ------------------------------------------------------------
    asset_key: List[str] = Field(
        ...,
        description="The asset's key path (AssetKey serialized as list of strings).",
    )
    location: Optional[str] = Field(
        default=None,
        description="Code location name. Set by the caller of extract_records().",
    )
    group: Optional[str] = Field(
        default=None,
        description="Dagster asset group name.",
    )

    # --- compute -------------------------------------------------------------
    is_executable: Optional[bool] = Field(
        default=None,
        description=(
            "False for assets Dagster cannot materialize: explicit AssetSpecs "
            "(external/source assets) and the stub assets Dagster auto-creates "
            "for a `deps=[AssetKey(...)]` pointing outside the Definitions — "
            "e.g. the source tables a dlt or Sling pipeline reads from. "
            "Selecting one in a run is rejected outright with "
            "`Selected keys must be a subset of existing executable asset "
            "keys`, so qualification classifies these OBSERVE_ONLY rather "
            "than launching them. None means the survey predates this field "
            "(schema_version < 2) or the lookup failed; readers MUST treat "
            "None as 'unknown', never as False."
        ),
    )
    compute_kind: Optional[str] = Field(
        default=None,
        description="Compute kind tag (e.g. 'python', 'dbt', 'sql').",
    )
    code_version: Optional[str] = Field(
        default=None,
        description="Asset code_version, when set on the asset.",
    )

    # --- IO management -------------------------------------------------------
    io_manager_key: Optional[str] = Field(
        default=None,
        description="Resource key of the IO manager handling this asset.",
    )
    io_manager_class: Optional[str] = Field(
        default=None,
        description=(
            "Fully-qualified class name of the IO manager resource (e.g. "
            "'dag_tools.io_managers.arrow.ConfigurableArrowIOManager'). The "
            "strongest signal for classification — survey consumes this directly."
        ),
    )
    io_manager_family: Optional[str] = Field(
        default=None,
        description=(
            "Coarse family classification ('postgres', 's3_iceberg', etc.) "
            "derived from io_manager_class via the classifier registry "
            "(FQN -> family with MRO walking). None when no match. The Broker "
            "consumes this for routing."
        ),
    )

    # --- partitioning --------------------------------------------------------
    partitions_def_class: Optional[str] = Field(
        default=None,
        description="FQN of the PartitionsDefinition class, if the asset is partitioned.",
    )
    partition_mapping_classes: List[str] = Field(
        default_factory=list,
        description="FQNs of every PartitionMapping class referenced in this asset's deps.",
    )

    # --- resources & integrations -------------------------------------------
    resource_keys: List[str] = Field(
        default_factory=list,
        description="Resource keys required by this asset (includes io_manager_key).",
    )
    resource_classes: Dict[str, str] = Field(
        default_factory=dict,
        description="Mapping from resource key to FQN of the resource class, joined from defs.resources.",
    )
    integration_libs: List[str] = Field(
        default_factory=list,
        description=(
            "Top-level dagster_* integration packages this asset touches, "
            "derived from module paths of its IO manager and resource classes "
            "(e.g. 'dagster_dbt', 'dagster_aws', 'dagster_dlt')."
        ),
    )

    # --- automation, freshness, backfill ------------------------------------
    automation_condition_type: Optional[str] = Field(
        default=None,
        description="FQN of the AutomationCondition subclass, if any.",
    )
    freshness_policy: Optional[Dict] = Field(
        default=None,
        description="Best-effort dict serialization of the asset's freshness policy.",
    )
    backfill_policy: Optional[Dict] = Field(
        default=None,
        description="Best-effort dict serialization of the asset's backfill policy.",
    )

    # --- tags, checks, jobs --------------------------------------------------
    tags: Dict[str, str] = Field(
        default_factory=dict,
        description="Asset tags (notably 'regression: true' for qualification selection).",
    )
    has_asset_checks: bool = Field(
        default=False,
        description="True if at least one AssetCheck targets this asset.",
    )
    job_names: List[str] = Field(
        default_factory=list,
        description="Names of jobs that reference this asset.",
    )

    # --- cross-system sidecar -----------------------------------------------
    urn: Optional[str] = Field(
        default=None,
        description=(
            "DataHub URN derived via dag_tools.components.datahub_lineage's "
            "asset_keys_to_dataset_urn_converter, when available. Lets the "
            "qualification system later cross-check inventory vs. live Broker "
            "heartbeats — registry says repo R at SHA S has asset X; do live "
            "Brokers actually heartbeat URN(X)? — without coupling the two "
            "systems' storage."
        ),
    )

"""Dagster-facing configuration schema for one labelled source dataset.

House shape, same as `tagged_warehouse.schema` and `read_audit.schema` and for
the same reason: the component's own attributes are plain scalars and
`Dict[str, Any]`, and these models are what those dicts are validated against
at the code-location load. The 1.10.19 compat floor rejects shapes the current
resolver handles, and its refusal fails the whole code location rather than
the one block -- so no attribute here is a nested `Resolvable`.

Validating at load is most of the value. A source that names no relation, an
access mode nobody implements, a `dagster`-platform URN standing in for the
asset of record -- each of those would otherwise surface as a published
dataset the catalog describes and no broker can authorize a read of, which
reads to an operator as a permissions problem rather than a configuration one.

Nothing here names a database, schema, table, column, dimension or value.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dag_tools.warehouse_source.observe import plan_observation
from dag_tools.warehouse_source.spec import SourceSpec, load_source_spec


class ObservationBlock(BaseModel):
    """How the source is looked at. It is never read from.

    Every statement is overridable because this library owns no dialect. The
    defaults are an aggregate and an empty projection, which is what makes an
    observation a look rather than a copy; see
    `dag_tools.warehouse_source.observe`.
    """

    model_config = ConfigDict(extra="forbid")

    count_rows: bool = Field(
        default=True,
        description=(
            "Whether to count the rows. Turn it off for a relation where a "
            "count is expensive or meaningless; the observation then records "
            "the schema and the as-of only, and the data version still moves "
            "when either of those changes."
        ),
    )
    row_count_statement: Optional[str] = Field(
        default=None,
        description=(
            "Your own count statement, with {table} where the relation goes. "
            "It must project exactly one value in exactly one row -- which of "
            "several columns was the count is never guessed."
        ),
    )
    columns_statement: Optional[str] = Field(
        default=None,
        description=(
            "Your own schema statement, with {table} where the relation goes. "
            "It should match no rows: the column names come off the result's "
            "own metadata, so an empty projection is enough."
        ),
    )
    as_of_statement: Optional[str] = Field(
        default=None,
        description=(
            "Optional. Your own statement projecting one value that says how "
            "fresh the source is -- a watermark, a load id, a max timestamp. "
            "Recorded as written: reformatting it would make two equal "
            "watermarks look different. Without it the as-of is the time of "
            "the observation, which says when the source was looked at and "
            "nothing about the data."
        ),
    )


class ClassificationBlock(BaseModel):
    """Where the dimensions document is read from. Not what is in it."""

    model_config = ConfigDict(extra="forbid")

    dimensions_file: Optional[str] = Field(
        default=None,
        description=(
            "Path to the deployment's classification dimensions document. "
            "Normally unset: the default is the mount, overridden by the "
            "DAGTOOLS_CLASSIFICATION_LEVELS environment variable. Setting it "
            "here beats that variable, so an ambient value left over from "
            "another deployment cannot silently change which document a label "
            "is validated against. dag-tools ships no such document."
        ),
    )
    require_ratified: bool = Field(
        default=False,
        description=(
            "Refuse to publish while any value the labels use carries no "
            "`ratified_by`. Off by default, because a deployment may be "
            "standing the document up before the board has signed each value; "
            "on, for a deployment whose labels are meant to be final."
        ),
    )


class CatalogBlock(BaseModel):
    """Where the published dataset goes. Nothing is emitted without a server."""

    model_config = ConfigDict(extra="forbid")

    server: Optional[str] = Field(
        default=None,
        description=(
            "The catalog's GMS URL. Unset means nothing is emitted -- the "
            "observation still runs and still records the whole label set in "
            "its own metadata, which is what lets this component be exercised "
            "with no catalog in front of it."
        ),
    )
    token: Optional[str] = Field(
        default=None,
        description=(
            "Catalog PAT. Required when the metadata service runs with "
            "METADATA_SERVICE_AUTH_ENABLED=true, which is the correct posture; "
            "falls back to the standard DATAHUB_TOKEN environment variable. "
            "Pass it as an env reference, never a literal."
        ),
    )
    description: Optional[str] = Field(
        default=None,
        description="Description published on the dataset.",
    )
    emit_lineage_from_asset: bool = Field(
        default=True,
        description=(
            "Attach this Dagster asset to the physical dataset as lineage. The "
            "dataset itself is the asset of record; the asset is what observed "
            "it. Off only if something else in your deployment already draws "
            "that edge."
        ),
    )


class WarehouseSourceSpec(BaseModel):
    """The validated whole, as `build_defs` resolves it."""

    model_config = ConfigDict(extra="forbid")

    asset_name: str
    key_prefix: List[str] = Field(default_factory=list)
    group_name: Optional[str] = None
    tags: Dict[str, str] = Field(default_factory=dict)
    resource_key: str = "warehouse"
    dry_run: bool = False
    source: SourceSpec
    observation: ObservationBlock = Field(default_factory=ObservationBlock)
    classification: ClassificationBlock = Field(default_factory=ClassificationBlock)
    catalog: CatalogBlock = Field(default_factory=CatalogBlock)

    @model_validator(mode="after")
    def _is_observable(self) -> "WarehouseSourceSpec":
        if not self.asset_name.strip():
            raise ValueError("the component must name the asset it builds")
        if not self.resource_key.strip():
            raise ValueError(
                "the component must name the warehouse resource it observes "
                "through; the observation is a read and is tagged like one"
            )
        # Prove the statements render now, rather than inside a materialization
        # at 3am. Rendering opens nothing -- that split is why it is safe here.
        plan_observation(
            table=self.source.table,
            row_count_statement=self.observation.row_count_statement,
            columns_statement=self.observation.columns_statement,
            as_of_statement=self.observation.as_of_statement,
        )
        return self

    @classmethod
    def from_attributes(
        cls,
        *,
        asset_name: str,
        key_prefix: Optional[List[str]],
        group_name: Optional[str],
        tags: Optional[Dict[str, str]],
        resource_key: str,
        dry_run: bool,
        source: Dict[str, Any],
        observation: Optional[Dict[str, Any]],
        classification: Optional[Dict[str, Any]],
        catalog: Optional[Dict[str, Any]],
    ) -> "WarehouseSourceSpec":
        return cls(
            asset_name=asset_name,
            key_prefix=list(key_prefix or []),
            group_name=group_name,
            tags=dict(tags or {}),
            resource_key=resource_key,
            dry_run=dry_run,
            source=load_source_spec(source or {}),
            observation=ObservationBlock.model_validate(observation or {}),
            classification=ClassificationBlock.model_validate(classification or {}),
            catalog=CatalogBlock.model_validate(catalog or {}),
        )

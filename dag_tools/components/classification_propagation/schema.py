"""Dagster-facing configuration schema for downstream label propagation.

House shape, same as `warehouse_source.schema` and for the same reason: the
component's own attributes are plain scalars and `Dict[str, Any]`, and these
models are what those dicts are validated against at the code-location load.
The 1.10.19 compat floor rejects shapes the current resolver handles, and its
refusal fails the whole code location rather than the one block -- so no
attribute here is a nested `Resolvable`.

What is worth validating at load is mostly the same three things as step 4a.
A derived dataset that names no labelled source would propagate from nothing;
a `dagster`-platform URN would publish the labels against the asset instead of
the dataset a broker is asked to authorize a read of; and a downgrade rule
with no owner or no reason is a standing permission to publish data wider
than it derives, with nobody's name on it. Each of those reads to an operator
as a permissions problem later and as a typo now.

**The sources are declared, not inferred from the asset graph.** Which
upstream assets carry labels is a policy fact rather than a graph fact: not
every dep publishes a label set, and an upstream that publishes none would
otherwise be treated as nothing to propagate from -- which composes a ceiling
out of a subset of the inputs and looks exactly like a correct answer.

Nothing here names a database, schema, table, column, dimension or value.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dag_tools.components.warehouse_source.schema import ClassificationBlock
from dag_tools.warehouse_source import REFUSED_PLATFORM, platform_of

#: The two statuses `DefaultSensorStatus` spells. Validated here so a typo
#: fails the YAML rather than defaulting to the quieter of the two.
SENSOR_STATUSES = ("RUNNING", "STOPPED")


def _asset_key_parts(text: str) -> List[str]:
    """`a/b` -> `["a", "b"]`, the spelling every other component uses.

    An empty segment is refused rather than dropped: `a//b` and `a/b` are two
    different asset keys, and silently reading the first as the second points
    the propagation at an asset nobody configured.
    """
    parts = [part for part in (text or "").split("/")]
    if not parts or any(not part.strip() for part in parts):
        raise ValueError(
            f"{text!r} is not an asset key; spell it as the key's parts "
            "separated by '/', with no empty part"
        )
    return [part.strip() for part in parts]


class DowngradeBlock(BaseModel):
    """A standing permission to publish one column wider than it derives.

    `owner` and `reason` are required because the rule is the only thing that
    can make a published label less restrictive than its sources. The two
    questions an auditor asks of one -- who stood behind it, and why -- are
    answered where the decision is declared rather than reconstructed from a
    commit log later.
    """

    model_config = ConfigDict(extra="forbid")

    id: str = Field(
        description=(
            "Stable id for the rule. It travels on every application, so an "
            "auditor can ask which rule published a column and get an answer "
            "that survives the YAML being reorganized."
        )
    )
    owner: str = Field(
        description=(
            "Who stands behind the rule. A person or a role, whatever your "
            "governance records -- this library does not interpret it."
        )
    )
    reason: str = Field(
        description="Why publishing wider than the data derives is allowed."
    )
    columns: List[str] = Field(
        description=(
            "The output columns the rule applies to. One rule per column: "
            "two rules reaching the same column are refused rather than "
            "ordered, because which one the record credits decides who is "
            "accountable."
        )
    )
    label: Dict[str, Any] = Field(
        description=(
            "What the column publishes as instead, `{<dimension>: "
            "<value|values>}`. It must include the hierarchical dimension and "
            "may say nothing about the others -- a dimension the rule omits "
            "keeps what the data derived. Validated against the mounted "
            "document like any other label, and refused if it would make any "
            "dimension more restrictive, which is a re-label rather than a "
            "downgrade."
        )
    )

    @model_validator(mode="after")
    def _is_accountable(self) -> "DowngradeBlock":
        for field in ("id", "owner", "reason"):
            if not str(getattr(self, field) or "").strip():
                raise ValueError(
                    f"a downgrade rule must name its {field}; a standing "
                    "permission to publish data wider than it derives, with "
                    "no id, owner or reason, cannot be audited or withdrawn"
                )
        if not self.columns or any(not str(c or "").strip() for c in self.columns):
            raise ValueError(
                f"downgrade rule {self.id!r} applies to no column. A rule "
                "nothing applies to is a permission nobody is watching"
            )
        if not self.label:
            raise ValueError(
                f"downgrade rule {self.id!r} declares no label, so there is "
                "nothing it would publish instead"
            )
        return self


class DatasetBlock(BaseModel):
    """One derived dataset whose columns are labelled from what they read."""

    model_config = ConfigDict(extra="forbid")

    asset_key: str = Field(
        description=(
            "The derived asset, as its key parts joined by '/'. Something "
            "else builds it -- dbt, dlt, a Python asset -- and this component "
            "neither owns nor materializes it."
        )
    )
    dataset_urn: str = Field(
        description=(
            "Where the derived data physically lives: the PHYSICAL platform's "
            "dataset URN, never a `dagster` one, for the reason step 4a "
            "records -- the catalog would describe one object and a broker "
            "asked to authorize a read of the other would find no labels."
        )
    )
    sources: List[str] = Field(
        description=(
            "The upstream assets whose published label sets this dataset's "
            "columns are composed from, as '/'-joined keys. Declared rather "
            "than read off the asset graph: see the module docstring."
        )
    )
    description: Optional[str] = Field(
        default=None, description="Description published on the dataset."
    )
    downgrades: List[DowngradeBlock] = Field(
        default_factory=list,
        description=(
            "Standing downgrade rules for this dataset's columns. No "
            "downgrade is automatic; absent a rule, a column publishes what "
            "it derives."
        ),
    )

    @model_validator(mode="after")
    def _is_derived_from_something(self) -> "DatasetBlock":
        key = _asset_key_parts(self.asset_key)
        platform = platform_of(self.dataset_urn)
        if platform == REFUSED_PLATFORM:
            raise ValueError(
                f"the derived dataset may not be a {REFUSED_PLATFORM!r} "
                f"platform URN ({self.dataset_urn!r}). The labels are "
                "published against the dataset a reader is authorized "
                "against, which is the physical one"
            )
        if not self.sources:
            raise ValueError(
                f"{self.asset_key!r} names no labelled source, so there is "
                "nothing to propagate from. A derived dataset with no "
                "labelled input is not derived -- label it at the source "
                "instead"
            )
        seen = set()
        for source in self.sources:
            parts = _asset_key_parts(source)
            if parts == key:
                raise ValueError(
                    f"{self.asset_key!r} lists itself as a source; a "
                    "dataset's labels cannot be composed from its own"
                )
            text = "/".join(parts)
            if text in seen:
                raise ValueError(f"{self.asset_key!r} lists {text!r} twice")
            seen.add(text)
        claimed: Dict[str, str] = {}
        for rule in self.downgrades:
            if rule.id in claimed.values():
                raise ValueError(
                    f"{self.asset_key!r} declares two downgrade rules called "
                    f"{rule.id!r}"
                )
            for column in rule.columns:
                if column in claimed:
                    raise ValueError(
                        f"{self.asset_key!r} has two downgrade rules for "
                        f"column {column!r} ({claimed[column]!r} and "
                        f"{rule.id!r}); which one the record credits decides "
                        "who is accountable, so it is declared rather than "
                        "ordered"
                    )
                claimed[column] = rule.id
        return self

    @property
    def key_parts(self) -> List[str]:
        return _asset_key_parts(self.asset_key)

    @property
    def source_keys(self) -> List[str]:
        return ["/".join(_asset_key_parts(source)) for source in self.sources]

    @property
    def platform(self) -> str:
        return platform_of(self.dataset_urn)


class PropagationCatalogBlock(BaseModel):
    """Where the propagated labels go. Nothing is emitted without a server."""

    model_config = ConfigDict(extra="forbid")

    server: Optional[str] = Field(
        default=None,
        description=(
            "The catalog's GMS URL. Unset means nothing is emitted -- every "
            "decision is still recorded against the derived asset, which is "
            "what lets this component be exercised with no catalog in front "
            "of it."
        ),
    )
    token: Optional[str] = Field(
        default=None,
        description=(
            "Catalog PAT. Required when the metadata service runs with "
            "METADATA_SERVICE_AUTH_ENABLED=true; falls back to the standard "
            "DATAHUB_TOKEN environment variable. Pass it as an env "
            "reference, never a literal."
        ),
    )


class ClassificationPropagationSpec(BaseModel):
    """The validated whole, as `build_defs` resolves it."""

    model_config = ConfigDict(extra="forbid")

    datasets: List[DatasetBlock]
    classification: ClassificationBlock = Field(
        default_factory=ClassificationBlock
    )
    catalog: PropagationCatalogBlock = Field(
        default_factory=PropagationCatalogBlock
    )
    sensor_name: str = "classification_propagation"
    minimum_interval_seconds: Optional[int] = None
    default_status: str = "STOPPED"
    dry_run: bool = False

    @model_validator(mode="after")
    def _propagates_something(self) -> "ClassificationPropagationSpec":
        if not self.datasets:
            raise ValueError(
                "the component must name at least one derived dataset to "
                "propagate labels for"
            )
        seen = set()
        for dataset in self.datasets:
            text = "/".join(dataset.key_parts)
            if text in seen:
                raise ValueError(
                    f"{text!r} is configured twice; two blocks for one asset "
                    "would publish two label sets against one dataset and "
                    "the later one would win silently"
                )
            seen.add(text)
        if not self.sensor_name.strip():
            raise ValueError("the component must name the sensor it builds")
        if self.default_status not in SENSOR_STATUSES:
            raise ValueError(
                f"default_status must be one of {list(SENSOR_STATUSES)}, not "
                f"{self.default_status!r}"
            )
        if self.minimum_interval_seconds is not None and (
            self.minimum_interval_seconds < 1
        ):
            raise ValueError("minimum_interval_seconds must be at least 1")
        return self

    @classmethod
    def from_attributes(
        cls,
        *,
        datasets: List[Dict[str, Any]],
        classification: Optional[Dict[str, Any]],
        catalog: Optional[Dict[str, Any]],
        sensor_name: str,
        minimum_interval_seconds: Optional[int],
        default_status: str,
        dry_run: bool,
    ) -> "ClassificationPropagationSpec":
        return cls(
            datasets=[
                DatasetBlock.model_validate(dataset or {})
                for dataset in (datasets or [])
            ],
            classification=ClassificationBlock.model_validate(classification or {}),
            catalog=PropagationCatalogBlock.model_validate(catalog or {}),
            sensor_name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=default_status,
            dry_run=dry_run,
        )

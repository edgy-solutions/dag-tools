"""What the catalog is told about a labelled source.

Pure: a spec, a resolution and an observation in; a `CatalogPayload` out. No
DataHub import -- the payload is plain data, and the component turns it into
aspects. That split is deliberate: the interesting decisions here are *which*
label each column publishes and what records the document it was published
against, and neither should be testable only with a catalog running.

Two properties carry the classification:

* **`classification_origin`** -- `board` or `catalog_tag`, straight from the
  spec. A reader who cannot tell a board decision from a tag somebody added
  in the catalog UI cannot tell how much the label is worth.
* **`classification`** -- the whole per-column label set as one JSON
  document, including the columns nobody labelled and the digest of the
  document the labels were validated against. Published as data rather than
  spread across one property per column, because the question asked of it is
  always "what was the label set at the time", never "what is column X".

The glossary terms are the same labels in DataHub's own vocabulary, attached
per column, which is what makes them visible in the catalog UI and queryable
in its graph.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .dimensions import DimensionSet
from .observe import Observation
from .spec import ResolvedLabels, SourceSpec

#: Where the labels came from.
CLASSIFICATION_ORIGIN_PROPERTY = "classification_origin"

#: The label set itself, as a JSON document.
CLASSIFICATION_PROPERTY = "classification"


@dataclass(frozen=True)
class ColumnPublication:
    """One column as the catalog will carry it."""

    name: str
    terms: Tuple[str, ...]
    label: Dict[str, List[str]]
    cell_predicate: Optional[str] = None
    defaulted: bool = False

    def as_json(self) -> Dict[str, Any]:
        document: Dict[str, Any] = {"labels": self.label, "terms": list(self.terms)}
        if self.cell_predicate:
            document["cell_predicate"] = self.cell_predicate
        if self.defaulted:
            # Recorded, because "most restrictive because nobody said" and
            # "most restrictive because the board said so" are the same label
            # and very different facts.
            document["defaulted"] = True
        return document


@dataclass(frozen=True)
class CatalogPayload:
    """Everything the catalog is told, as data."""

    dataset_urn: str
    platform: str
    custom_properties: Dict[str, str]
    columns: Tuple[ColumnPublication, ...]
    description: Optional[str] = None

    @property
    def column_terms(self) -> Dict[str, List[str]]:
        return {column.name: list(column.terms) for column in self.columns}

    @property
    def defaulted_columns(self) -> Tuple[str, ...]:
        return tuple(column.name for column in self.columns if column.defaulted)

    def as_json(self) -> Dict[str, Any]:
        return {
            "dataset_urn": self.dataset_urn,
            "platform": self.platform,
            "description": self.description,
            "custom_properties": dict(self.custom_properties),
            "columns": {
                column.name: column.as_json() for column in self.columns
            },
        }


def build_catalog_payload(
    *,
    spec: SourceSpec,
    resolved: ResolvedLabels,
    observation: Observation,
    dimensions: DimensionSet,
    asset_key: Optional[str] = None,
    description: Optional[str] = None,
) -> CatalogPayload:
    """Describe the published source.

    Every column the source *has* appears, not every column the operator
    listed: a column nobody labelled is published as most restrictive, and a
    column published with no label at all would read as unclassified --
    therefore readable -- which is the one reading this build exists to
    prevent.
    """
    columns: List[ColumnPublication] = []
    defaulted = set(resolved.defaulted)
    for name, label in resolved.labels:
        columns.append(
            ColumnPublication(
                name=name,
                terms=dimensions.terms_for(label),
                label=label.as_dict(),
                cell_predicate=resolved.predicate_for(name),
                defaulted=name in defaulted,
            )
        )
    classification = {
        "dimensions_source": dimensions.source,
        "dimensions_digest": dimensions.digest,
        "columns": {column.name: column.as_json() for column in columns},
    }
    properties: Dict[str, str] = {
        CLASSIFICATION_ORIGIN_PROPERTY: spec.label_origin,
        CLASSIFICATION_PROPERTY: json.dumps(
            classification, sort_keys=True, separators=(",", ":")
        ),
        "access": spec.access,
        "owner": spec.owner,
        "table": spec.table,
        "schema_hash": observation.schema_hash,
        "as_of": observation.as_of,
        "observed_at": observation.observed_at,
        "data_version": observation.data_version,
        "column_count": str(len(columns)),
        "defaulted_column_count": str(len(defaulted)),
    }
    if observation.row_count is not None:
        properties["row_count"] = str(observation.row_count)
    if dimensions.digest:
        properties["classification_digest"] = dimensions.digest
    if dimensions.source:
        properties["classification_source"] = dimensions.source
    if asset_key:
        # Which Dagster asset observes it. The lineage edge itself is the
        # dep edge in the asset graph -- this is so the catalog can say who
        # to ask about the observation without walking the graph.
        properties["dagster_asset_key"] = asset_key
    return CatalogPayload(
        dataset_urn=spec.dataset_urn,
        platform=spec.platform,
        custom_properties=properties,
        columns=tuple(columns),
        description=description,
    )

"""One source dataset, as the operator declares it, validated.

Pure: the YAML block in, a `SourceSpec` out, and -- once the mounted
document is in hand -- a `LabelledSource` whose every label is one the
document declares. No Dagster, no warehouse, no IO.

**The asset of record is the physical platform's dataset URN.** Ruled
2026-11-05: `s3` for a file-backed fixture, the warehouse's own platform for
a warehouse source, never a `dagster` platform URN. A `dagster` URN is
refused here rather than merely discouraged, because the failure it causes is
invisible: the catalog would describe one object, a crawler would discover a
second, and a broker asked to authorize a read of the physical one would find
no labels on it.

Nothing in this module names a database, schema, table, column, dimension or
value.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .dimensions import DimensionSet, DimensionsError, Label

#: How a read of this source reaches it. `brokered` reads are authorized per
#: request by the broker; `passthrough` reads are the caller's own session.
#: The component publishes which one applies, because a reader that cannot
#: tell them apart cannot tell whether a denial is expected.
ACCESS_MODES = ("brokered", "passthrough")

#: Where the labels came from: the governance board's own decision, or a tag
#: already carried in the catalog. Published as `classification_origin`.
LABEL_ORIGINS = ("board", "catalog_tag")

DATASET_URN_PREFIX = "urn:li:dataset:("
PLATFORM_URN_PREFIX = "urn:li:dataPlatform:"

#: The one platform a source dataset may not be registered under.
REFUSED_PLATFORM = "dagster"


class SourceSpecError(ValueError):
    """The declared source cannot be published as described."""


def platform_of(dataset_urn: str) -> str:
    """The platform id inside a dataset URN.

    A small parser rather than a dependency on DataHub's own: this module is
    pure, and the one thing it has to be able to say about a URN -- which
    platform it names -- must be sayable with the catalog library absent.
    """
    urn = (dataset_urn or "").strip()
    if not urn.startswith(DATASET_URN_PREFIX) or not urn.endswith(")"):
        raise SourceSpecError(
            f"{urn!r} is not a dataset URN; the asset of record is spelled "
            f"{DATASET_URN_PREFIX}{PLATFORM_URN_PREFIX}<platform>,<name>,<env>)"
        )
    inner = urn[len(DATASET_URN_PREFIX) : -1]
    parts = [part.strip() for part in inner.split(",")]
    if len(parts) < 2 or not parts[0].startswith(PLATFORM_URN_PREFIX):
        raise SourceSpecError(
            f"{urn!r} names no platform; expected a leading "
            f"{PLATFORM_URN_PREFIX}<platform>"
        )
    platform = parts[0][len(PLATFORM_URN_PREFIX) :]
    if not platform:
        raise SourceSpecError(f"{urn!r} names an empty platform")
    return platform


def env_of(dataset_urn: str) -> str:
    """The fabric a dataset URN names.

    Needed because the lineage node for the observing asset has to sit in the
    same fabric as the source, and the fabric is a deployment's own word for
    its environment -- so it is read off the operator's URN rather than named
    here.
    """
    platform_of(dataset_urn)  # same grammar, same refusals, one place
    inner = dataset_urn.strip()[len(DATASET_URN_PREFIX) : -1]
    parts = [part.strip() for part in inner.split(",")]
    if len(parts) < 3 or not parts[-1]:
        raise SourceSpecError(
            f"{dataset_urn!r} names no fabric; expected a trailing environment "
            "element"
        )
    return parts[-1]


class ColumnLabel(BaseModel):
    """One column's classification label, and optionally its row filter."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(description="The column, as the source spells it.")
    labels: Dict[str, Any] = Field(
        description=(
            "The label as {dimension: values}. The hierarchical dimension is "
            "required and takes exactly one value; every other dimension "
            "takes one value or a list, read as the document's `type:` for "
            "that dimension says -- a reader holds all of a restrictive "
            "set, at least one of a permissive set, none of an exclusionary "
            "one."
        )
    )
    cell_predicate: Optional[str] = Field(
        default=None,
        description=(
            "Your own SQL predicate narrowing which rows of this column a "
            "read may see. Published as part of the label; never interpreted "
            "here, because this library owns no dialect."
        ),
    )

    @model_validator(mode="after")
    def _is_declared_fully(self) -> "ColumnLabel":
        if not self.name or not self.name.strip():
            raise ValueError("a labelled column must name the column")
        if not self.labels:
            raise ValueError(
                f"column {self.name!r} declares an empty label; a column with "
                "no entry at all is most restrictive in every dimension, and "
                "an empty entry must not be mistaken for one"
            )
        if self.cell_predicate is not None and not self.cell_predicate.strip():
            raise ValueError(
                f"column {self.name!r} declares an empty cell_predicate; omit "
                "the key instead, so an empty filter is never read as 'all rows'"
            )
        return self


class SourceSpec(BaseModel):
    """One source dataset: where it is, what is on it, and who owns it."""

    model_config = ConfigDict(extra="forbid")

    dataset_urn: str = Field(
        description=(
            "The asset of record: the PHYSICAL platform's dataset URN. Never a "
            "`dagster` platform URN -- the Dagster asset attaches as lineage."
        )
    )
    table: str = Field(
        description=(
            "The relation the observation reads, as your warehouse spells it "
            "(db.schema.table or whatever it takes). No default."
        )
    )
    access: str = Field(
        description="How reads reach it: " + " | ".join(ACCESS_MODES) + "."
    )
    owner: str = Field(description="Who owns the source. Published to the catalog.")
    label_origin: str = Field(
        description="Where the labels came from: " + " | ".join(LABEL_ORIGINS) + "."
    )
    columns: List[ColumnLabel] = Field(
        default_factory=list,
        description=(
            "The labelled columns. A column of the source that is not listed "
            "here is published as most restrictive in every dimension -- deny "
            "by default -- so an incomplete list under-serves rather than "
            "over-serves."
        ),
    )

    @model_validator(mode="after")
    def _is_publishable(self) -> "SourceSpec":
        platform = platform_of(self.dataset_urn)
        if platform == REFUSED_PLATFORM:
            raise ValueError(
                f"the asset of record may not be a {REFUSED_PLATFORM!r} platform "
                f"URN ({self.dataset_urn!r}). The catalog registers the source "
                "under the platform it physically lives on, and the Dagster "
                "asset attaches to that as lineage, so that a broker "
                "authorizing a read and a catalog describing it name one object"
            )
        if not self.table.strip():
            raise ValueError("the source must name the relation to observe")
        if self.access not in ACCESS_MODES:
            raise ValueError(
                f"access must be one of {list(ACCESS_MODES)}; got {self.access!r}"
            )
        if self.label_origin not in LABEL_ORIGINS:
            raise ValueError(
                f"label_origin must be one of {list(LABEL_ORIGINS)}; got "
                f"{self.label_origin!r}"
            )
        if not self.owner.strip():
            raise ValueError("the source must name an owner")
        folded = [column.name.strip().lower() for column in self.columns]
        duplicates = sorted({name for name in folded if folded.count(name) > 1})
        if duplicates:
            raise ValueError(
                f"the same column is labelled more than once ({duplicates}); two "
                "labels on one column would publish whichever the reader "
                "resolved last"
            )
        return self

    @property
    def platform(self) -> str:
        return platform_of(self.dataset_urn)


@dataclass(frozen=True)
class ResolvedLabels:
    """Every observed column's label, declared or defaulted."""

    labels: Tuple[Tuple[str, Label], ...]
    defaulted: Tuple[str, ...]
    predicates: Tuple[Tuple[str, str], ...]

    def as_dict(self) -> Dict[str, Dict[str, List[str]]]:
        return {column: label.as_dict() for column, label in self.labels}

    def predicate_for(self, column: str) -> Optional[str]:
        for name, predicate in self.predicates:
            if name == column:
                return predicate
        return None


@dataclass(frozen=True)
class LabelledSource:
    """A spec whose every declared label the mounted document declares too.

    Built at the code-location load, so a label naming a dimension or a value
    policy does not declare fails while the operator is still looking at the
    YAML -- and built again at observation time, because the document is the
    authority at the moment of the read, not at the moment of the deploy.
    """

    spec: SourceSpec
    dimensions: DimensionSet
    declared: Tuple[Tuple[str, Label], ...]

    def resolve(self, observed_columns: Tuple[str, ...]) -> ResolvedLabels:
        """Label every column the source actually has.

        Matching is case-insensitive because a warehouse returns identifiers
        in its own case and the operator's YAML cannot be made to guess which:
        the published spelling is the source's, the declared spelling is only
        how it was found. A fold that collides is refused rather than resolved
        -- two columns differing only in case would otherwise take one
        another's label.
        """
        folded_observed: Dict[str, List[str]] = {}
        for column in observed_columns:
            folded_observed.setdefault(column.strip().lower(), []).append(column)
        collisions = sorted(
            key for key, names in folded_observed.items() if len(names) > 1
        )
        if collisions:
            raise SourceSpecError(
                "the source has columns that differ only by case "
                f"({[folded_observed[key] for key in collisions]}); a label "
                "cannot be attached to one of them without risking the other"
            )
        declared_by_fold = {
            name.strip().lower(): (name, label) for name, label in self.declared
        }
        missing = sorted(
            declared_by_fold[key][0]
            for key in declared_by_fold
            if key not in folded_observed
        )
        if missing:
            raise SourceSpecError(
                f"the source does not have the labelled columns {missing}. The "
                "labels are stale: whatever those columns were renamed to is "
                "now published as most restrictive, so no read of it would be "
                "served, and that is worth failing loudly for rather than "
                "publishing a label set that describes the wrong table"
            )
        fallback = self.dimensions.most_restrictive_label()
        labels: List[Tuple[str, Label]] = []
        defaulted: List[str] = []
        predicates: List[Tuple[str, str]] = []
        declared_predicates = {
            column.name.strip().lower(): column.cell_predicate
            for column in self.spec.columns
            if column.cell_predicate
        }
        for column in observed_columns:
            key = column.strip().lower()
            if key in declared_by_fold:
                labels.append((column, declared_by_fold[key][1]))
            else:
                labels.append((column, fallback))
                defaulted.append(column)
            predicate = declared_predicates.get(key)
            if predicate:
                predicates.append((column, predicate))
        return ResolvedLabels(
            labels=tuple(labels),
            defaulted=tuple(defaulted),
            predicates=tuple(predicates),
        )


def label_source(spec: SourceSpec, dimensions: DimensionSet) -> LabelledSource:
    """Validate every declared label against the mounted document.

    Raises on the first disagreement rather than collecting them: unlike a
    mapping, where an operator wants the whole list of problems at once, a
    label that does not validate means the document and the YAML disagree
    about what classification *is* -- and the rest of the list is unlikely to
    be independent of that.
    """
    if dimensions.is_empty:
        raise DimensionsError(
            "the mounted classification document declares no dimensions, so no "
            "label can be validated against it; a source cannot be published "
            "unlabelled"
        )
    declared: List[Tuple[str, Label]] = []
    for column in spec.columns:
        declared.append(
            (
                column.name,
                dimensions.validate_label(
                    column.labels, where=f"column {column.name!r}"
                ),
            )
        )
    return LabelledSource(
        spec=spec, dimensions=dimensions, declared=tuple(declared)
    )


def load_source_spec(block: Mapping[str, Any]) -> SourceSpec:
    """Validate the operator's YAML block. One validator, load and run."""
    try:
        return SourceSpec.model_validate(dict(block))
    except SourceSpecError:
        raise
    except Exception as exc:  # pydantic ValidationError, mostly
        raise SourceSpecError(str(exc)) from exc

"""The classification dimensions document, as data.

Pure: a parsed document in, a validated `DimensionSet` out. The component
reads the mount and hands the document here, which is what lets every rule
in `AGENTS.md` §4l be tested without a file, a ConfigMap or a cluster.

**This module names no dimension and no value.** It cannot: the document is
the governance board's decision and arrives at runtime, so the dimension that
orders the others is discovered from its own `hierarchical` flag rather than
from a constant here. A dimension id compiled into this library would turn a
board decision into a library release -- the same rule `dag_tools.audit`
follows for relation and column names.

Three refusals are worth knowing before reading the code, because each one
exists to stop a *silent* failure rather than a loud one:

* **Exactly one dimension is hierarchical.** The ceiling rule ("a clearance
  implies every less restrictive value"), "most restrictive" and the
  downstream `max` are each defined only on a ladder. A document declaring
  none, or two, is ambiguous in a way no code should resolve by picking one,
  so it is refused.
* **A declared dimension with no values makes the document unusable.** Not
  "unrestricted": a dimension nothing is ratified on has no most restrictive
  value, so the deny-by-default label cannot be built, and a reader must be
  served nothing rather than something.
* **A label must carry the hierarchical dimension.** A column with no label
  at all is most restrictive everywhere (`most_restrictive_label`); a label
  that *omits* the ladder is an operator mistake, and defaulting it is how a
  column gets labelled by accident.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

#: Env var naming the mounted document. Unchanged when the document became a
#: set of dimensions rather than one ordered list of levels: the hierarchical
#: dimension *is* that former list, so the deployment rebuilds the
#: ConfigMap's content and not its wiring.
DIMENSIONS_ENV_VAR = "DAGTOOLS_CLASSIFICATION_LEVELS"

#: Where the deployment mounts it when the env var says nothing. A mount
#: *location*, not a name from below -- which is the only reason anything in
#: this package carries a default at all.
DEFAULT_DIMENSIONS_PATH = "/etc/dagtools/classification/levels.yaml"

#: The document's one top-level key.
DOCUMENT_KEY = "classification_dimensions"

#: Prefix of a derived glossary term URN. The *shape* is DataHub's; the ids
#: inside come from the document.
TERM_URN_PREFIX = "urn:li:glossaryTerm:"


class DimensionsError(ValueError):
    """The mounted document cannot be used, or a label disagrees with it."""


class DimensionValue(BaseModel):
    """One ratified value of one dimension.

    `extra="ignore"` for the same reason `AssetRecord` does it: policy owns
    this document and may add fields to it between dag-tools releases, and a
    reader that refused an unknown field would make every such addition a
    coordinated rollout.
    """

    model_config = ConfigDict(extra="ignore")

    id: str = Field(description="The value's id, as the policy document spells it.")
    reason: Optional[str] = Field(
        default=None, description="Why this value exists. Recorded by policy."
    )
    ratified_by: Optional[str] = Field(
        default=None, description="Who ratified it. Recorded by policy."
    )
    term_urn: Optional[str] = Field(
        default=None,
        description=(
            "The catalog glossary term this value publishes as. When policy "
            "declares none, one is derived from the dimension and value ids."
        ),
    )

    @model_validator(mode="after")
    def _id_is_present(self) -> "DimensionValue":
        if not self.id or not self.id.strip():
            raise ValueError("a dimension value must carry a non-empty id")
        return self

    def term(self, dimension_id: str) -> str:
        """The term URN this value publishes as.

        Derived when policy declares none. The derivation composes two ids
        the document supplies; it invents no name of its own.
        """
        return self.term_urn or f"{TERM_URN_PREFIX}{dimension_id}.{self.id}"


class Dimension(BaseModel):
    """One dimension and its ratified values, in the document's own order."""

    model_config = ConfigDict(extra="ignore")

    id: str = Field(description="The dimension's id, as the policy document spells it.")
    hierarchical: bool = Field(
        default=False,
        description=(
            "True for the one dimension whose values form a ladder, ordered "
            "least restrictive first. Exactly one dimension sets it."
        ),
    )
    values: List[DimensionValue] = Field(
        default_factory=list,
        description=(
            "The ratified values, least restrictive first for the "
            "hierarchical dimension. Legitimately empty before anything is "
            "ratified -- which makes the document unusable, not permissive."
        ),
    )

    @model_validator(mode="after")
    def _ids_are_present_and_distinct(self) -> "Dimension":
        if not self.id or not self.id.strip():
            raise ValueError("a dimension must carry a non-empty id")
        seen = set()
        for value in self.values:
            if value.id in seen:
                raise ValueError(
                    f"dimension {self.id!r} declares the value {value.id!r} twice; "
                    "a repeated value makes its position -- and so its rank -- "
                    "ambiguous"
                )
            seen.add(value.id)
        return self

    @property
    def value_ids(self) -> Tuple[str, ...]:
        return tuple(value.id for value in self.values)

    def rank(self, value_id: str) -> int:
        """Position in the document. Only meaningful on the ladder."""
        try:
            return self.value_ids.index(value_id)
        except ValueError:
            raise DimensionsError(
                f"the mounted document does not declare {value_id!r} on dimension "
                f"{self.id!r}; it declares {list(self.value_ids)}"
            ) from None

    def value(self, value_id: str) -> DimensionValue:
        return self.values[self.rank(value_id)]

    def most_restrictive(self) -> Tuple[str, ...]:
        """The most restrictive label this dimension can carry.

        On the ladder that is its last row. On an unordered dimension it is
        *every* value: a label's values are conjunctive, so requiring them
        all is the narrowest a column can be -- and the deny-by-default
        reading of a column nobody labelled.
        """
        if not self.values:
            raise DimensionsError(
                f"dimension {self.id!r} declares no values, so it has no most "
                "restrictive one; the mounted document cannot be used"
            )
        if self.hierarchical:
            return (self.value_ids[-1],)
        return self.value_ids


@dataclass(frozen=True)
class Label:
    """A validated classification label: dimension id -> ratified values.

    Ordered by the document, and the values inside each dimension ordered by
    their position in it, so two operators who wrote the same label in a
    different order publish the same bytes.
    """

    pairs: Tuple[Tuple[str, Tuple[str, ...]], ...]

    def as_dict(self) -> Dict[str, List[str]]:
        return {dimension: list(values) for dimension, values in self.pairs}

    def values_for(self, dimension_id: str) -> Tuple[str, ...]:
        for dimension, values in self.pairs:
            if dimension == dimension_id:
                return values
        return ()

    def __bool__(self) -> bool:
        return bool(self.pairs)


@dataclass(frozen=True)
class DimensionSet:
    """The mounted document, validated.

    `source` and `digest` carry where it came from and a digest of the bytes
    it came as. They travel into the observation's metadata and the catalog
    payload: the one question an auditor asks of a published label is which
    document it was published against, and the answer has to be recorded at
    the moment of publishing rather than looked up afterwards, when the
    ConfigMap may have moved on.
    """

    dimensions: Tuple[Dimension, ...]
    source: Optional[str] = None
    digest: Optional[str] = None

    # -- construction --------------------------------------------------------

    @classmethod
    def from_document(
        cls,
        document: Any,
        *,
        source: Optional[str] = None,
        digest: Optional[str] = None,
    ) -> "DimensionSet":
        if document is None:
            return cls(dimensions=(), source=source, digest=digest)
        if not isinstance(document, Mapping):
            raise DimensionsError(
                "the mounted classification document must be a mapping with a "
                f"{DOCUMENT_KEY!r} key; got {type(document).__name__}"
            )
        if DOCUMENT_KEY not in document:
            raise DimensionsError(
                f"the mounted classification document has no {DOCUMENT_KEY!r} key; "
                f"its top-level keys are {sorted(str(k) for k in document)}"
            )
        rows = document.get(DOCUMENT_KEY) or []
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            raise DimensionsError(
                f"{DOCUMENT_KEY!r} must be a list of dimensions; got "
                f"{type(rows).__name__}"
            )
        dimensions: List[Dimension] = []
        for index, row in enumerate(rows):
            try:
                dimensions.append(Dimension.model_validate(row))
            except ValidationError as exc:
                raise DimensionsError(
                    f"{DOCUMENT_KEY}[{index}] is not a usable dimension: {exc}"
                ) from exc
        return cls(
            dimensions=tuple(dimensions), source=source, digest=digest
        )._validated()

    def _validated(self) -> "DimensionSet":
        ids = [dimension.id for dimension in self.dimensions]
        duplicates = sorted({i for i in ids if ids.count(i) > 1})
        if duplicates:
            raise DimensionsError(
                f"the mounted document declares {duplicates} more than once; a "
                "repeated dimension makes a label's meaning depend on which "
                "copy a reader happened to resolve"
            )
        if self.dimensions:
            ladders = [d.id for d in self.dimensions if d.hierarchical]
            if len(ladders) != 1:
                raise DimensionsError(
                    "exactly one dimension must set `hierarchical: true`; the "
                    f"mounted document sets it on {ladders or 'none of them'}. "
                    "The ceiling rule, 'most restrictive' and the downstream "
                    "max are each defined only on a ladder, so this cannot be "
                    "resolved by picking one"
                )
        return self

    # -- shape ---------------------------------------------------------------

    @property
    def is_empty(self) -> bool:
        """Nothing is declared. Deny by default: nothing can be served."""
        return not self.dimensions

    @property
    def is_usable(self) -> bool:
        """Every declared dimension has at least one ratified value.

        An unusable document is not a permissive one. It has no most
        restrictive label to fall back to, so a column cannot be labelled
        deny-by-default either -- and a source whose labels cannot be
        resolved must publish nothing rather than publish them unlabelled.
        """
        return bool(self.dimensions) and all(d.values for d in self.dimensions)

    @property
    def unratified(self) -> Tuple[str, ...]:
        """Declared dimensions with nothing ratified on them."""
        return tuple(d.id for d in self.dimensions if not d.values)

    @property
    def values_without_ratification(self) -> Tuple[str, ...]:
        """Declared values carrying no `ratified_by`, as `dimension.value`.

        A different question from `unratified`, which is about a dimension
        with no values at all: these values exist and can be labelled with,
        they simply have nobody's name against them yet. Reported rather than
        refused, because a deployment standing the document up before the
        board has signed each value is a legitimate state -- the component's
        `require_ratified` is how a deployment whose labels are meant to be
        final opts into refusing it.
        """
        return tuple(
            f"{dimension.id}.{value.id}"
            for dimension in self.dimensions
            for value in dimension.values
            if not (value.ratified_by or "").strip()
        )

    @property
    def ids(self) -> Tuple[str, ...]:
        return tuple(d.id for d in self.dimensions)

    @property
    def hierarchical(self) -> Dimension:
        for dimension in self.dimensions:
            if dimension.hierarchical:
                return dimension
        raise DimensionsError(
            "the mounted document declares no hierarchical dimension, so it "
            "declares no ladder to order clearances on"
        )

    def get(self, dimension_id: str) -> Dimension:
        for dimension in self.dimensions:
            if dimension.id == dimension_id:
                return dimension
        raise DimensionsError(
            f"the mounted document does not declare the dimension "
            f"{dimension_id!r}; it declares {list(self.ids)}"
        )

    # -- labels --------------------------------------------------------------

    def validate_label(self, label: Any, *, where: str = "a column") -> Label:
        """Validate `{dimension: values}` against the document.

        Every refusal here is an operator mistake with a one-line fix, which
        is why they are raised rather than noted: the component turns them
        into a failure of the code-location load, where an operator is still
        looking at the YAML they just wrote.
        """
        if not self.is_usable:
            raise DimensionsError(
                "the mounted classification document is not usable, so no label "
                "can be validated against it"
                + (
                    f"; nothing is ratified on {list(self.unratified)}"
                    if self.unratified
                    else ""
                )
            )
        if not isinstance(label, Mapping) or not label:
            raise DimensionsError(
                f"{where} must carry a non-empty label of the form "
                "{dimension: values}; a column with no label at all is most "
                f"restrictive in every dimension, but {where} declares an "
                "empty one, and the two cannot be told apart"
            )
        resolved: Dict[str, Tuple[str, ...]] = {}
        for dimension_id, raw in label.items():
            dimension = self.get(str(dimension_id))
            values = _as_values(raw, dimension_id=dimension.id, where=where)
            if dimension.hierarchical and len(values) != 1:
                raise DimensionsError(
                    f"{where} gives {len(values)} values on the hierarchical "
                    f"dimension {dimension.id!r}; it takes exactly one. A list "
                    "on a ladder is ambiguous -- a range, or a maximum? -- so "
                    "it is refused rather than interpreted"
                )
            ranked = sorted({value: dimension.rank(value) for value in values}.items(),
                            key=lambda item: item[1])
            resolved[dimension.id] = tuple(value for value, _ in ranked)
        ladder = self.hierarchical
        if ladder.id not in resolved:
            raise DimensionsError(
                f"{where} does not carry the hierarchical dimension "
                f"{ladder.id!r}, which every label must. A column with no label "
                "at all is most restrictive everywhere; a half-written label is "
                "not defaulted, because that is how a column gets labelled by "
                "accident"
            )
        return self._ordered(resolved)

    def most_restrictive_label(self) -> Label:
        """The label of a column nobody labelled. Deny by default."""
        if not self.is_usable:
            raise DimensionsError(
                "the mounted classification document is not usable, so there is "
                "no most restrictive label to fall back to"
                + (
                    f"; nothing is ratified on {list(self.unratified)}"
                    if self.unratified
                    else ""
                )
            )
        return self._ordered(
            {dimension.id: dimension.most_restrictive() for dimension in self.dimensions}
        )

    def terms_for(self, label: Label) -> Tuple[str, ...]:
        """The glossary term URNs a label publishes as, document order."""
        terms: List[str] = []
        for dimension_id, values in label.pairs:
            dimension = self.get(dimension_id)
            for value_id in values:
                terms.append(dimension.value(value_id).term(dimension.id))
        return tuple(terms)

    def _ordered(self, resolved: Mapping[str, Tuple[str, ...]]) -> Label:
        return Label(
            pairs=tuple(
                (dimension.id, tuple(resolved[dimension.id]))
                for dimension in self.dimensions
                if dimension.id in resolved
            )
        )


def _as_values(raw: Any, *, dimension_id: str, where: str) -> Tuple[str, ...]:
    if isinstance(raw, str):
        candidates: Sequence[Any] = (raw,)
    elif isinstance(raw, Sequence) and not isinstance(raw, (bytes, bytearray)):
        candidates = list(raw)
    else:
        raise DimensionsError(
            f"{where} gives {type(raw).__name__} for dimension {dimension_id!r}; "
            "a label's values are one value or a list of them"
        )
    if not candidates:
        raise DimensionsError(
            f"{where} gives no value for dimension {dimension_id!r}; an empty "
            "list is not 'unrestricted', so it is refused rather than dropped"
        )
    values: List[str] = []
    for candidate in candidates:
        if not isinstance(candidate, str) or not candidate.strip():
            raise DimensionsError(
                f"{where} gives {candidate!r} for dimension {dimension_id!r}; "
                "a value is the non-empty id the mounted document declares"
            )
        values.append(candidate)
    return tuple(values)


def resolve_dimensions_path(
    explicit: Optional[str] = None, environ: Optional[Mapping[str, str]] = None
) -> str:
    """Where to read the document from: explicit config, then env, then mount.

    Explicit configuration beats the environment on purpose, the same way
    `dest_config` does elsewhere in this repo: an ambient variable left over
    from another deployment must never silently retarget a read -- and here
    what it would retarget is which document a label is validated against.
    """
    if explicit and str(explicit).strip():
        return str(explicit)
    environ = os.environ if environ is None else environ
    from_env = environ.get(DIMENSIONS_ENV_VAR)
    if from_env and from_env.strip():
        return from_env
    return DEFAULT_DIMENSIONS_PATH

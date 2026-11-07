"""Propagation: what a derived column's label is, given its sources'.

Pure, like `dimensions`: labels and lineage in, a decision per output column
out. No dagster, no DataHub, no warehouse -- which is what lets every rule
below be tested without a cluster, and lets the same engine serve the
materialisation sensor, a CI seal and a dry run.

**This module names no dimension, no value and no column.** The composition
rule for a dimension is the dimension's own (`Dimension.most_restrictive_of`);
all this module knows is how to walk lineage and which columns to ask about.

Four things here are worth knowing before reading the code, because each one
is a *silent* failure the shape of the code exists to prevent:

* **A derived label is total.** Every dimension the document declares appears
  in it, even one no source mentioned. On a permissive set an absent
  dimension means "no restriction" and an empty one means "nobody", and both
  would read as the empty tuple -- so a derived label never omits one, and
  the empty tuple there always means nobody.
* **An unlabelled source fails.** Not "unrestricted", and not deny-by-default
  either: deny-by-default is the reading of a *source* column nobody
  labelled, applied where the source is declared. By the time propagation
  runs, a source column with no label means lineage and labels disagree
  about what was read, and composing the ones that happen to be present
  publishes a ceiling computed from a subset of the inputs.
* **Lineage absent for a column falls back to the table max**, never to
  nothing. A Python asset that declares no column lineage has read the whole
  table as far as anyone can prove, so every output column carries the join
  of every labelled column it could have come from.
* **No downgrade is automatic.** A downgrade exists only as a declared rule
  carrying a named owner and a reason, and applying one records the rule,
  the owner, the reason and both labels. A rule missing either, or one that
  does not actually relax what was derived, is refused rather than recorded:
  an aspect saying "downgraded" that nobody can account for is worse than no
  aspect at all.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from dag_tools.warehouse_source.dimensions import (
    DimensionSet,
    DimensionsError,
    Label,
)

#: The column's label came from its own declared column lineage.
COLUMN_LINEAGE = "column_lineage"

#: No column lineage was declared for it, so it carries the join of every
#: labelled column of every source read. The conservative reading, and the
#: one `TableColumnLineage` metadata exists to narrow.
TABLE_MAX = "table_max"

#: Every basis a decision can carry.
BASES = (COLUMN_LINEAGE, TABLE_MAX)


class PropagationError(ValueError):
    """Lineage, labels and declarations disagree, or a downgrade is unaccountable.

    Separate from `DimensionsError` on purpose: that one means the mounted
    document or a label disagrees with it, which an operator fixes in YAML.
    This one means the *graph* does not add up, which an operator fixes in an
    asset's metadata or a downgrade declaration.
    """


@dataclass(frozen=True)
class ColumnRef:
    """One source column, as whoever declared the lineage spelled it."""

    dataset: str
    column: str

    def __str__(self) -> str:  # pragma: no cover - diagnostics only
        return f"{self.dataset}.{self.column}"


@dataclass(frozen=True)
class DowngradeRule:
    """A standing permission to publish a column wider than it derives.

    `owner` and `reason` are not decoration. The rule is the only thing that
    can make a published label less restrictive than its sources, so the two
    questions an auditor asks of one -- who stood behind it, and why -- are
    answered where the decision is made rather than reconstructed later.
    """

    id: str
    owner: str
    reason: str
    label: Mapping[str, Any]
    columns: Tuple[str, ...] = ()

    @property
    def column_ids(self) -> Tuple[str, ...]:
        return tuple(self.columns)


@dataclass(frozen=True)
class AppliedDowngrade:
    """One application of one rule, recorded in full.

    Both labels travel: the derived one is what the data says, the declared
    one is what was published, and an aspect carrying only the second cannot
    be checked against the first.
    """

    rule: str
    owner: str
    reason: str
    column: str
    derived: Label
    declared: Label

    def as_dict(self) -> Dict[str, Any]:
        return {
            "rule": self.rule,
            "owner": self.owner,
            "reason": self.reason,
            "column": self.column,
            "derived": self.derived.as_dict(),
            "declared": self.declared.as_dict(),
        }


@dataclass(frozen=True)
class ColumnDecision:
    """What one output column publishes as, and how that was arrived at."""

    column: str
    label: Label
    basis: str
    sources: Tuple[ColumnRef, ...] = ()
    downgrade: Optional[AppliedDowngrade] = None

    @property
    def derived(self) -> Label:
        """The label the sources compose to, before any downgrade."""
        return self.downgrade.derived if self.downgrade else self.label

    @property
    def is_downgraded(self) -> bool:
        return self.downgrade is not None

    def as_dict(self) -> Dict[str, Any]:
        row: Dict[str, Any] = {
            "column": self.column,
            "label": self.label.as_dict(),
            "basis": self.basis,
            "sources": [str(source) for source in self.sources],
        }
        if self.downgrade is not None:
            row["downgrade"] = self.downgrade.as_dict()
        return row


@dataclass(frozen=True)
class Propagation:
    """Every output column's decision, in the order they were asked for."""

    columns: Tuple[ColumnDecision, ...] = ()

    @property
    def labels(self) -> Dict[str, Label]:
        return {decision.column: decision.label for decision in self.columns}

    @property
    def downgrades(self) -> Tuple[AppliedDowngrade, ...]:
        return tuple(
            decision.downgrade
            for decision in self.columns
            if decision.downgrade is not None
        )

    @property
    def table_max_columns(self) -> Tuple[str, ...]:
        """Columns that fell back, so an operator can see what lineage would buy."""
        return tuple(d.column for d in self.columns if d.basis == TABLE_MAX)

    def as_dict(self) -> Dict[str, Any]:
        return {"columns": [decision.as_dict() for decision in self.columns]}


# -- composition -------------------------------------------------------------


def compose(
    dimensions: DimensionSet, labels: Sequence[Label], *, where: str = "a column"
) -> Label:
    """The narrowest label admitting only readers every one of `labels` admits.

    Per dimension this is `Dimension.most_restrictive_of` over the labels
    that *declare* it. Skipping the ones that do not is not a shortcut: an
    undeclared dimension is the identity of its own join -- the empty set
    where the join unions, the whole set where it intersects -- so including
    it would change nothing. The result still carries every dimension, with
    the dimension's least restrictive value where nothing declared it,
    because an omitted dimension and an empty one read alike and mean
    opposite things on a permissive set.
    """
    if not dimensions.is_usable:
        raise DimensionsError(
            "the mounted classification document is not usable, so no label "
            "can be composed from it"
            + (
                f"; nothing is ratified on {list(dimensions.unratified)}"
                if dimensions.unratified
                else ""
            )
        )
    if not labels:
        raise PropagationError(
            f"{where} has no source labels to compose; composing none would "
            "yield the least restrictive label in every dimension, which is "
            "the one answer that must never be reached by accident"
        )
    pairs: List[Tuple[str, Tuple[str, ...]]] = []
    for dimension in dimensions.dimensions:
        declared = [
            label.values_for(dimension.id)
            for label in labels
            if label.declares(dimension.id)
        ]
        pairs.append((dimension.id, dimension.most_restrictive_of(declared)))
    ladder = dimensions.hierarchical
    composed = Label(pairs=tuple(pairs))
    if not composed.values_for(ladder.id):
        raise PropagationError(
            f"{where} composes to no value on the hierarchical dimension "
            f"{ladder.id!r}; every label carries it, so this means a source "
            "label was built without being validated against the document"
        )
    return composed


# -- propagation -------------------------------------------------------------


def propagate(
    dimensions: DimensionSet,
    *,
    output_columns: Sequence[str],
    inputs: Mapping[str, Mapping[str, Any]],
    lineage: Optional[Mapping[str, Sequence[ColumnRef]]] = None,
    downgrades: Sequence[DowngradeRule] = (),
    where: str = "an asset",
) -> Propagation:
    """Compose each output column's label from the source columns it read.

    `inputs` is dataset -> column -> label, each label either already
    validated or the raw `{dimension: values}` the source declared, which is
    validated here -- so a value outside its dimension's vocabulary fails at
    the same place every other document disagreement does.

    `lineage` is output column -> the source columns it came from. A column
    missing from it, or naming no sources, carries the table max instead:
    the join of every labelled column of every dataset in `inputs`.
    """
    resolved = _validated_inputs(dimensions, inputs, where=where)
    if not resolved:
        raise PropagationError(
            f"{where} reads no labelled source column, so there is nothing to "
            "propagate from. An asset with no labelled inputs is not derived; "
            "running propagation over it would publish a ceiling composed "
            "from none of its inputs"
        )
    rules = _validated_rules(dimensions, downgrades, output_columns, where=where)
    lineage = lineage or {}
    table_max: Optional[Label] = None

    decisions: List[ColumnDecision] = []
    for column in output_columns:
        sources = tuple(lineage.get(column) or ())
        if sources:
            labels = [
                _source_label(resolved, source, column=column, where=where)
                for source in sources
            ]
            derived = compose(dimensions, labels, where=f"{where} column {column!r}")
            basis = COLUMN_LINEAGE
        else:
            if table_max is None:
                table_max = compose(
                    dimensions,
                    [label for columns in resolved.values() for label in columns.values()],
                    where=f"{where} (table max)",
                )
            derived, basis = table_max, TABLE_MAX
        decisions.append(
            _with_downgrade(
                dimensions,
                ColumnDecision(
                    column=column, label=derived, basis=basis, sources=sources
                ),
                rules.get(column),
                where=where,
            )
        )
    return Propagation(columns=tuple(decisions))


def _with_downgrade(
    dimensions: DimensionSet,
    decision: ColumnDecision,
    rule: Optional[Tuple[DowngradeRule, Label]],
    *,
    where: str,
) -> ColumnDecision:
    if rule is None:
        return decision
    declaration, rule_label = rule
    declared = _overlay(dimensions, decision.label, rule_label)
    if declared == decision.label:
        raise PropagationError(
            f"downgrade rule {declaration.id!r} publishes {where} column "
            f"{decision.column!r} as exactly what it already derives; a "
            "recorded decision that changed nothing tells an auditor a "
            "downgrade happened when none did, so the rule is refused "
            "rather than applied"
        )
    tightened = [
        dimension.id
        for dimension in dimensions.dimensions
        if not dimension.at_most_as_restrictive_as(
            declared.values_for(dimension.id), decision.label.values_for(dimension.id)
        )
    ]
    if tightened:
        raise PropagationError(
            f"downgrade rule {declaration.id!r} makes {where} column "
            f"{decision.column!r} *more* restrictive on {tightened}. That is "
            "a re-label, not a downgrade: it would be recorded as a release "
            "of data that was never released, and a reader denied by it would "
            "have no rule to appeal to"
        )
    return ColumnDecision(
        column=decision.column,
        label=declared,
        basis=decision.basis,
        sources=decision.sources,
        downgrade=AppliedDowngrade(
            rule=declaration.id,
            owner=declaration.owner,
            reason=declaration.reason,
            column=decision.column,
            derived=decision.label,
            declared=declared,
        ),
    )


# -- validation --------------------------------------------------------------


def _overlay(dimensions: DimensionSet, derived: Label, declared: Label) -> Label:
    """The published label: the derived one, with the rule's dimensions replaced.

    A rule's label is a *declared* label, so like every other it carries the
    ladder and whichever other dimensions it means to change. A dimension it
    stays silent about keeps what the data derived -- it is not reset, and it
    is certainly not emptied. Comparing a partial declaration against a total
    derived label directly would read every unmentioned dimension as a
    tightening, so a rule relaxing one dimension could never be written
    without restating all of them.
    """
    return Label(
        pairs=tuple(
            (
                dimension.id,
                declared.values_for(dimension.id)
                if declared.declares(dimension.id)
                else derived.values_for(dimension.id),
            )
            for dimension in dimensions.dimensions
        )
    )


def _validated_inputs(
    dimensions: DimensionSet, inputs: Mapping[str, Mapping[str, Any]], *, where: str
) -> Dict[str, Dict[str, Label]]:
    resolved: Dict[str, Dict[str, Label]] = {}
    for dataset, columns in (inputs or {}).items():
        if not isinstance(columns, Mapping):
            raise PropagationError(
                f"{where} gives {type(columns).__name__} as the labels of "
                f"source {dataset!r}; it takes a mapping of column to label"
            )
        rows: Dict[str, Label] = {}
        for column, raw in columns.items():
            rows[str(column)] = (
                raw
                if isinstance(raw, Label)
                else dimensions.validate_label(
                    raw, where=f"source column {dataset}.{column}"
                )
            )
        if rows:
            resolved[str(dataset)] = rows
    return resolved


def _validated_rules(
    dimensions: DimensionSet,
    downgrades: Sequence[DowngradeRule],
    output_columns: Sequence[str],
    *,
    where: str,
) -> Dict[str, Tuple[DowngradeRule, Label]]:
    known = set(output_columns)
    rules: Dict[str, Tuple[DowngradeRule, Label]] = {}
    for rule in downgrades or ():
        if not rule.id or not str(rule.id).strip():
            raise PropagationError(
                "a downgrade rule must carry an id; an application recorded "
                "against an unnamed rule cannot be traced back to the "
                "declaration that allowed it"
            )
        if not rule.owner or not str(rule.owner).strip():
            raise PropagationError(
                f"downgrade rule {rule.id!r} names no owner. A standing "
                "downgrade is somebody's decision, and a rule nobody owns "
                "cannot be withdrawn, reviewed or appealed to"
            )
        if not rule.reason or not str(rule.reason).strip():
            raise PropagationError(
                f"downgrade rule {rule.id!r} gives no reason. The reason is "
                "what the published aspect carries; without it the catalog "
                "records that data was released more widely than it derives "
                "and nothing about why"
            )
        if not rule.column_ids:
            raise PropagationError(
                f"downgrade rule {rule.id!r} names no columns, so it can "
                "never be applied. A declared rule that does nothing reads "
                "as cover for a release it never granted"
            )
        declared = dimensions.validate_label(
            rule.label, where=f"downgrade rule {rule.id!r}"
        )
        for column in rule.column_ids:
            if column not in known:
                raise PropagationError(
                    f"downgrade rule {rule.id!r} names column {column!r}, "
                    f"which {where} does not produce; it produces "
                    f"{sorted(known)}. A rule aimed at a column that no "
                    "longer exists is a permission nobody is watching"
                )
            if column in rules:
                raise PropagationError(
                    f"downgrade rules {rules[column][0].id!r} and {rule.id!r} "
                    f"both apply to column {column!r}; which one the published "
                    "aspect credits would decide who is accountable for the "
                    "release, so it is refused rather than ordered"
                )
            rules[column] = (rule, declared)
    return rules


def _source_label(
    resolved: Mapping[str, Mapping[str, Label]],
    source: ColumnRef,
    *,
    column: str,
    where: str,
) -> Label:
    columns = resolved.get(source.dataset)
    if columns is None:
        matches = [
            dataset
            for dataset in resolved
            if dataset.casefold() == source.dataset.casefold()
        ]
        if len(matches) == 1:
            columns = resolved[matches[0]]
        elif matches:
            raise PropagationError(
                f"{where} column {column!r} reads {source}, and "
                f"{sorted(matches)} all match that source name but for case; "
                "which one it meant decides the label, so it is refused"
            )
        else:
            raise PropagationError(
                f"{where} column {column!r} declares lineage from "
                f"{source}, but no labels were read for source "
                f"{source.dataset!r}. Lineage and labels disagree about what "
                f"was read; {where} knows of {sorted(resolved)}"
            )
    return _column_label(columns, source, column=column, where=where)


def _column_label(
    columns: Mapping[str, Label], source: ColumnRef, *, column: str, where: str
) -> Label:
    if source.column in columns:
        return columns[source.column]
    # A warehouse that upper-cases its identifiers, or a loader that
    # snake-cases them, hands back a different spelling of the same column.
    # One case-insensitive match is that column; two is a question only the
    # source can answer, so it is refused rather than picked.
    matches = [
        name for name in columns if name.casefold() == source.column.casefold()
    ]
    if len(matches) == 1:
        return columns[matches[0]]
    if matches:
        raise PropagationError(
            f"{where} column {column!r} reads {source}, and {sorted(matches)} "
            "all match that column name but for case; which one it meant "
            "decides the label, so it is refused"
        )
    raise PropagationError(
        f"{where} column {column!r} reads {source}, which carries no label. "
        "An unlabelled source column is not an unrestricted one: composing "
        "only the sources that happen to be labelled would publish a ceiling "
        f"computed from a subset of the inputs. Source {source.dataset!r} "
        f"labels {sorted(columns)}"
    )

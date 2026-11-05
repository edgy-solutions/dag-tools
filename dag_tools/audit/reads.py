"""What the service identity read, and which asset explains it.

Step 1 of the classification build, and deliberately the first thing built:
before anything is labelled, the board needs the size of the problem -- how
many objects the pipelines touch, which asset touches each, and how many are
touched by nothing anyone can name. That last number is the one that decides
how much of step 3 is registry work and how much is archaeology.

Pure on purpose. No warehouse client, no Dagster import, no IO. The adapter
that fetches history rows lives in `.history` and the command in `.cli`; this
module is the part with the judgment in it, so it is the part that stays
testable without a warehouse.

**How a read is attributed, in order:**

  1. The query tag names an `asset_key` -- definite.
  2. The tag names a `run_id` -- look the run up. Definite only if that run
     materialized exactly ONE asset; a multi-asset run cannot say which of
     its assets issued the statement.
  3. Neither -- correlate by time: the read falls inside exactly one run
     window, and that run has exactly one asset. Any overlap, and it is
     ambiguous.

**Ambiguous is not explained.** A read we can place in a run but not on an
asset stays in the gap, because the remedy is the same as for an untagged
read: step 2's tag. Counting it as explained would make the number better
than the system is, and the number exists to be acted on.

**A read by a non-pipeline principal stays in the gap too, and is also listed
separately.** No asset explains a human's query, so it belongs in the total;
but its remedy is step 5's `passthrough`, not a tag, so a reader who needs to
split the work can. It is never silently excused -- an audit that quietly
drops the reads it cannot categorise is how a gap becomes invisible.

Every list is sorted and de-duplicated, so the same history produces the same
report and two windows can be diffed.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Set, Tuple

from pydantic import BaseModel, ConfigDict, Field

from .tag import QueryAttribution, decode, is_pipeline


SCHEMA_VERSION = 1
"""Monotonic version of `ReadsReport`. Additive-only.

History:
  1 — initial report (per-asset, per-relation, gap decomposition, methods).
"""

BY_ASSET_KEY = "asset_key"
"""Attributed from the tag's asset_key."""

BY_RUN_ID = "run_id"
"""Attributed from the tag's run_id, via a single-asset run."""

BY_CORRELATION = "correlation"
"""Attributed by falling inside exactly one single-asset run window."""

AMBIGUOUS = "ambiguous"
"""Placed in a run, or in several, but not on one asset."""

UNATTRIBUTED = "unattributed"
"""No tag, and no single-asset run window covering it. This is the number
the query tag (step 2) exists to drive to zero."""

NOT_A_PIPELINE = "not_a_pipeline"
"""The tag names a principal that is not a pipeline -- the gateway's verified
`on_behalf_of`, i.e. a person. No asset can explain it, and none should: what
a person may read is a brokering decision, not an attribution gap."""

METHODS = (
    BY_ASSET_KEY,
    BY_RUN_ID,
    BY_CORRELATION,
    AMBIGUOUS,
    UNATTRIBUTED,
    NOT_A_PIPELINE,
)

DEFINITE_METHODS = (BY_ASSET_KEY, BY_RUN_ID, BY_CORRELATION)


class ReadEvent(BaseModel):
    """One statement out of query history.

    `relations` holds whatever the history says the statement touched, as
    opaque strings. This module never parses, normalizes or interprets them --
    they are the operator's own object identifiers, and a report that rewrote
    them could not be joined back to the warehouse it came from.
    """

    model_config = ConfigDict(extra="ignore")

    query_id: str
    started_at: datetime
    principal: Optional[str] = None
    query_tag: Optional[str] = None
    relations: List[str] = Field(default_factory=list)


class RunWindow(BaseModel):
    """When a run ran, and what it materialized.

    An `ended_at` of None means still running, and the window is open to the
    right -- a read during a run in flight is still that run's read.
    """

    model_config = ConfigDict(extra="ignore")

    run_id: str
    asset_keys: List[str] = Field(default_factory=list)
    started_at: datetime
    ended_at: Optional[datetime] = None


class ReadsReport(BaseModel):
    """The three numbers that were asked for, plus the gap decomposed."""

    model_config = ConfigDict(extra="ignore")

    schema_version: int = SCHEMA_VERSION
    generated_at: datetime

    event_count: int
    relation_count: int

    relations_per_asset: Dict[str, List[str]] = Field(default_factory=dict)
    assets_per_relation: Dict[str, List[str]] = Field(default_factory=dict)

    explained_relations: List[str] = Field(default_factory=list)
    unexplained_relations: List[str] = Field(default_factory=list)

    ambiguous_relations: List[str] = Field(default_factory=list)
    untagged_relations: List[str] = Field(default_factory=list)
    non_pipeline_relations: List[str] = Field(default_factory=list)

    method_counts: Dict[str, int] = Field(default_factory=dict)
    events_without_relations: int = 0

    @property
    def coverage(self) -> Optional[float]:
        """Share of objects some asset explains, or None when nothing was
        read -- 0/0 is not full coverage and must never render as one."""
        if self.relation_count == 0:
            return None
        return len(self.explained_relations) / self.relation_count


def _window_contains(window: RunWindow, moment: datetime) -> bool:
    if moment < window.started_at:
        return False
    if window.ended_at is None:
        return True
    return moment <= window.ended_at


def _sole_asset(window: Optional[RunWindow]) -> Optional[str]:
    """The one asset a run materialized, or None if it is not exactly one."""
    if window is None:
        return None
    keys = sorted(set(window.asset_keys))
    return keys[0] if len(keys) == 1 else None


def attribute_event(
    event: ReadEvent,
    runs_by_id: Dict[str, RunWindow],
    windows: Sequence[RunWindow],
) -> Tuple[str, Optional[str], Optional[QueryAttribution]]:
    """Decide one event: `(method, asset_key or None, decoded tag or None)`.

    The tag is the only thing whose principal is read. The row says which
    credential connected, the tag says who the read was for, and under
    `passthrough` those differ on purpose -- but the row's identity is a name
    in the warehouse's own namespace, not in this contract's grammar, so it
    is evidence of neither.
    """
    tag = decode(event.query_tag)

    # Deny-by-default, but only over a value this library wrote. Testing the
    # ROW's identity here was wrong twice over: a warehouse username never
    # matches `pipeline:<name>`, so every untagged read was reported as "not
    # a pipeline" and the correlation branch below was unreachable. Narrowing
    # the read to the audited identity is the operator's `principal_filter`;
    # it is not an inference to make from an opaque credential name. An
    # unrecognised kind inside a tag still denies -- the tag is ours, so a
    # kind we do not know is a contract break and must not be attributed.
    if tag is not None and not is_pipeline(tag.principal):
        return NOT_A_PIPELINE, None, tag

    if tag is not None and tag.asset_key:
        return BY_ASSET_KEY, tag.asset_key, tag

    if tag is not None and tag.run_id:
        sole = _sole_asset(runs_by_id.get(tag.run_id))
        if sole is not None:
            return BY_RUN_ID, sole, tag
        return AMBIGUOUS, None, tag

    overlapping = [w for w in windows if _window_contains(w, event.started_at)]
    if len(overlapping) == 1:
        sole = _sole_asset(overlapping[0])
        if sole is not None:
            return BY_CORRELATION, sole, tag
        return AMBIGUOUS, None, tag
    if len(overlapping) > 1:
        return AMBIGUOUS, None, tag
    return UNATTRIBUTED, None, tag


def build_reads_report(
    events: Sequence[ReadEvent],
    runs: Sequence[RunWindow] = (),
    generated_at: Optional[datetime] = None,
) -> ReadsReport:
    """Roll one history window up into the report.

    `runs` may be empty; then nothing correlates and every untagged read lands
    in the gap, which is the honest reading of "we cannot say".
    """
    runs_by_id = {run.run_id: run for run in runs}
    windows = list(runs)

    per_asset: Dict[str, Set[str]] = {}
    per_relation: Dict[str, Set[str]] = {}
    methods: Dict[str, int] = {method: 0 for method in METHODS}

    all_relations: Set[str] = set()
    relations_ambiguous: Set[str] = set()
    relations_untagged: Set[str] = set()
    relations_non_pipeline: Set[str] = set()
    no_relation_events = 0

    for event in events:
        method, asset_key, _tag = attribute_event(event, runs_by_id, windows)
        methods[method] = methods.get(method, 0) + 1

        relations = sorted({r for r in event.relations if r})
        if not relations:
            no_relation_events += 1
            continue
        all_relations.update(relations)

        if asset_key is not None:
            per_asset.setdefault(asset_key, set()).update(relations)
            for relation in relations:
                per_relation.setdefault(relation, set()).add(asset_key)
        elif method == AMBIGUOUS:
            relations_ambiguous.update(relations)
        elif method == NOT_A_PIPELINE:
            relations_non_pipeline.update(relations)
        else:
            relations_untagged.update(relations)

    explained = set(per_relation)
    unexplained = all_relations - explained

    return ReadsReport(
        generated_at=generated_at or datetime.now(timezone.utc),
        event_count=len(events),
        relation_count=len(all_relations),
        relations_per_asset={
            asset: sorted(rels) for asset, rels in sorted(per_asset.items())
        },
        assets_per_relation={
            relation: sorted(assets)
            for relation, assets in sorted(per_relation.items())
        },
        explained_relations=sorted(explained),
        unexplained_relations=sorted(unexplained),
        ambiguous_relations=sorted(relations_ambiguous & unexplained),
        untagged_relations=sorted(relations_untagged & unexplained),
        non_pipeline_relations=sorted(relations_non_pipeline & unexplained),
        method_counts=methods,
        events_without_relations=no_relation_events,
    )

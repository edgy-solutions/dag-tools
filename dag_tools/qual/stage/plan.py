"""Pure staging plan: which computed upstreams to materialize, and in what runs.

No IO and no client. Input is the class matrix plus the deployment's
``launch_info`` snapshot; output is a :class:`StagePlan`. Everything the
runner launches, and everything it reports as left out, is decided here so
it can be tested without a deployment.

The selection is the transitive closure of computed upstreams of every
RUNNABLE representative that needs staging, cross-checked against what the
deployment can actually launch, then split into one group per partition
definition (a run cannot mix definitions; see the runner docs).
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from pydantic import BaseModel, ConfigDict, Field

from ..classes import ClassMatrix, ClassMember, Runnability, StagingNeed
from ..classes.staging import build_executability_index
from ..runs.launcher import last_partition_key
from ..runs.state import rep_id_for


Key = Tuple[str, ...]

UNPARTITIONED_GROUP = "unpartitioned"


class LaunchInfoUnavailable(RuntimeError):
    """The deployment reported nothing about its assets.

    Stage refuses to plan against an empty snapshot. A single unlaunchable
    key fails a whole run, and without the snapshot such a key cannot be
    detected.
    """


class KeyDependents(BaseModel):
    """One reported key and the representatives that depend on it."""
    model_config = ConfigDict(extra="ignore")

    asset_key: List[str]
    reps: List[str] = Field(default_factory=list)


class StageGroupPlan(BaseModel):
    """One run's worth of keys: a single partition definition."""
    model_config = ConfigDict(extra="ignore")

    group_key: str
    partition_key: Optional[str] = None
    selection: List[List[str]]
    depends_on: List[str] = Field(default_factory=list)


class StagePlan(BaseModel):
    """The full plan for one qualification (side-independent)."""
    model_config = ConfigDict(extra="ignore")

    selection_hash: str
    groups: List[StageGroupPlan] = Field(default_factory=list)
    key_count: int = 0
    unknown_reps: List[str] = Field(default_factory=list)
    unknown_closure_keys: List[List[str]] = Field(default_factory=list)
    unstageable_keys: List[KeyDependents] = Field(default_factory=list)
    uncovered_cross_group_keys: List[KeyDependents] = Field(default_factory=list)
    rep_requirements: Dict[str, List[List[str]]] = Field(default_factory=dict)
    """Representative id -> every key of its closure, including keys that
    are reported rather than launched, minus keys the deployment treats as
    external (there is nothing to stage for those). Drives "which reps are
    ready"."""


def build_stage_plan(
    matrix: ClassMatrix,
    launch_info: Dict[str, Dict[str, Any]],
) -> StagePlan:
    """Compute the plan. Raises :class:`LaunchInfoUnavailable` on an empty
    ``launch_info``."""
    if not launch_info:
        raise LaunchInfoUnavailable(
            "the deployment reported no asset launch info; refusing to plan "
            "a staging run it cannot validate"
        )

    members: List[ClassMember] = [m for c in matrix.classes for m in c.members]
    index = build_executability_index(members)  # reads asset_key / is_executable
    edges = _edges_by_key(members)
    member_by_identity = {(m.repo, tuple(m.asset_key)): m for m in members}

    unknown_reps: List[str] = []
    rep_closure: Dict[str, Set[Key]] = {}
    unknown_closure: Set[Key] = set()

    for cls in matrix.classes:
        for rep in cls.representatives:
            if rep.runnability != Runnability.RUNNABLE:
                continue
            rid = rep_id_for(cls.class_hash, rep.asset_key)
            if rep.staging_need == StagingNeed.UNKNOWN:
                unknown_reps.append(rid)
                continue
            if rep.staging_need != StagingNeed.REQUIRED:
                continue
            member = member_by_identity.get((rep.repo, tuple(rep.asset_key)))
            if member is None or member.upstream_asset_keys is None:
                unknown_reps.append(rid)
                continue
            closure, unknown = _walk(
                [tuple(u) for u in member.upstream_asset_keys], index, edges
            )
            rep_closure[rid] = closure
            unknown_closure |= unknown

    wanted: Set[Key] = set().union(*rep_closure.values()) if rep_closure else set()

    # --- Deployment cross-check -------------------------------------------
    unstageable: Set[Key] = set()
    external_here: Set[Key] = set()
    staged: Set[Key] = set()
    info_of: Dict[Key, Dict[str, Any]] = {}
    for key in wanted:
        info = launch_info.get("/".join(key))
        if info is None:
            unstageable.add(key)
        elif info.get("is_executable") is False:
            external_here.add(key)  # external here: nothing to materialize
        else:
            staged.add(key)
            info_of[key] = info

    # --- Grouping ---------------------------------------------------------
    group_of, group_partition = _assign_groups(staged, info_of)

    # --- Order, and the keys that sit on a cross-group cycle ---------------
    cycle_keys = _cross_group_cycle_keys(staged, edges, group_of)
    staged -= cycle_keys
    group_of = {k: g for k, g in group_of.items() if k in staged}

    group_deps = _group_dependencies(staged, edges, group_of)
    order = _topological_order(sorted(set(group_of.values())), group_deps)

    groups = [
        StageGroupPlan(
            group_key=g,
            partition_key=group_partition.get(g),
            selection=sorted(list(k) for k, gk in group_of.items() if gk == g),
            depends_on=sorted(group_deps.get(g, set())),
        )
        for g in order
    ]

    def dependents(keys: Iterable[Key]) -> List[KeyDependents]:
        return [
            KeyDependents(
                asset_key=list(k),
                reps=sorted(r for r, cl in rep_closure.items() if k in cl),
            )
            for k in sorted(keys)
        ]

    return StagePlan(
        selection_hash=_selection_hash(groups),
        groups=groups,
        key_count=sum(len(g.selection) for g in groups),
        unknown_reps=sorted(unknown_reps),
        unknown_closure_keys=sorted(list(k) for k in unknown_closure),
        unstageable_keys=dependents(unstageable),
        uncovered_cross_group_keys=dependents(cycle_keys),
        rep_requirements={
            r: sorted(list(k) for k in cl - external_here)
            for r, cl in sorted(rep_closure.items())
        },
    )


def reps_ready(
    plan: StagePlan, materialized: Set[Key]
) -> Tuple[List[str], List[str]]:
    """Split the planned representatives into (ready, not ready).

    A rep is ready when every key of its closure materialized. A key the
    plan reported instead of launching is never materialized, so it keeps
    its reps not ready.
    """
    ready: List[str] = []
    not_ready: List[str] = []
    for rid, keys in plan.rep_requirements.items():
        (ready if all(tuple(k) in materialized for k in keys) else not_ready).append(rid)
    return ready, not_ready


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _edges_by_key(members: List[ClassMember]) -> Dict[Key, Optional[Set[Key]]]:
    """Upstream keys per asset key, unioned over every pinned repo.

    ``None`` when no member of the key knows its upstreams (a survey that
    predates the field): unknown, never "no upstreams".
    """
    out: Dict[Key, Optional[Set[Key]]] = {}
    for m in members:
        key = tuple(m.asset_key)
        if m.upstream_asset_keys is None:
            out.setdefault(key, None)
            continue
        current = out.get(key)
        ups = {tuple(u) for u in m.upstream_asset_keys}
        out[key] = ups if current is None else current | ups
    return out


def _walk(
    seeds: List[Key],
    index: Dict[Key, Optional[bool]],
    edges: Dict[Key, Optional[Set[Key]]],
) -> Tuple[Set[Key], Set[Key]]:
    """Transitive computed ancestors of ``seeds``; stops at non-computed keys.

    Returns ``(closure, keys_whose_upstreams_are_unknown)``.
    """
    closure: Set[Key] = set()
    unknown: Set[Key] = set()
    stack = list(seeds)
    while stack:
        key = stack.pop()
        if key in closure or index.get(key) is not True:
            continue
        closure.add(key)
        ups = edges.get(key)
        if ups is None:
            unknown.add(key)
            continue
        stack.extend(ups)
    return closure, unknown


def _assign_groups(
    staged: Set[Key], info_of: Dict[Key, Dict[str, Any]]
) -> Tuple[Dict[Key, str], Dict[str, Optional[str]]]:
    """Group by the deployment's partition-key list (see module doc)."""
    partitioned: Dict[Tuple[str, ...], str] = {}
    key_group: Dict[Key, Optional[str]] = {}
    partition_of: Dict[str, Optional[str]] = {}
    for key in sorted(staged):
        info = info_of[key]
        if not info.get("is_partitioned"):
            key_group[key] = None
            continue
        pkeys = tuple(str(p) for p in info.get("partition_keys") or [])
        gk = partitioned.get(pkeys)
        if gk is None:
            gk = "partitioned-" + hashlib.sha256(
                json.dumps(pkeys).encode("utf-8")
            ).hexdigest()[:8]
            partitioned[pkeys] = gk
            partition_of[gk] = last_partition_key(info)
        key_group[key] = gk

    loose = [k for k, g in key_group.items() if g is None]
    if len(partitioned) == 1 and loose:
        # One partitioned group: unpartitioned keys ride along (measured to
        # work), which gives the common case of exactly one run per side.
        (only,) = partitioned.values()
        for k in loose:
            key_group[k] = only
    elif loose:
        partition_of[UNPARTITIONED_GROUP] = None
        for k in loose:
            key_group[k] = UNPARTITIONED_GROUP

    return {k: g for k, g in key_group.items() if g is not None}, partition_of


def _cross_edges(
    staged: Set[Key],
    edges: Dict[Key, Optional[Set[Key]]],
    group_of: Dict[Key, str],
) -> List[Tuple[Key, Key]]:
    """(upstream, downstream) pairs inside ``staged`` that cross groups."""
    out: List[Tuple[Key, Key]] = []
    for down in staged:
        for up in edges.get(down) or ():
            if up in staged and group_of[up] != group_of[down]:
                out.append((up, down))
    return out


def _group_dependencies(
    staged: Set[Key],
    edges: Dict[Key, Optional[Set[Key]]],
    group_of: Dict[Key, str],
) -> Dict[str, Set[str]]:
    deps: Dict[str, Set[str]] = {}
    for up, down in _cross_edges(staged, edges, group_of):
        deps.setdefault(group_of[down], set()).add(group_of[up])
    return deps


def _cross_group_cycle_keys(
    staged: Set[Key],
    edges: Dict[Key, Optional[Set[Key]]],
    group_of: Dict[Key, str],
) -> Set[Key]:
    """Endpoints of every cross-group edge whose two groups share a cycle."""
    cross = _cross_edges(staged, edges, group_of)
    succ: Dict[str, Set[str]] = {}
    for up, down in cross:
        succ.setdefault(group_of[up], set()).add(group_of[down])

    def reaches(src: str, dst: str) -> bool:
        seen: Set[str] = set()
        stack = [src]
        while stack:
            g = stack.pop()
            if g == dst:
                return True
            if g in seen:
                continue
            seen.add(g)
            stack.extend(succ.get(g, ()))
        return False

    bad: Set[Key] = set()
    for up, down in cross:
        # up's group feeds down's group; a path back closes the cycle.
        if reaches(group_of[down], group_of[up]):
            bad.update((up, down))
    return bad


def _topological_order(groups: List[str], deps: Dict[str, Set[str]]) -> List[str]:
    """Kahn's algorithm, ties broken by sorted group key."""
    remaining = {g: set(deps.get(g, ())) & set(groups) for g in groups}
    order: List[str] = []
    while remaining:
        ready = sorted(g for g, d in remaining.items() if not d)
        if not ready:  # unreachable: cycles were removed; stay total anyway
            ready = sorted(remaining)
        g = ready[0]
        order.append(g)
        del remaining[g]
        for d in remaining.values():
            d.discard(g)
    return order


def _selection_hash(groups: List[StageGroupPlan]) -> str:
    payload = json.dumps(
        sorted(
            [g.group_key, g.partition_key, g.selection] for g in groups
        ),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]

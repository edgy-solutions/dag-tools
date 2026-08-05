"""Representative selection + runnability classification.

Recipe rule (Phase Q1, item 4):

  > Representative selection per class: prefer assets tagged
  > ``regression: "true"``, else smallest known runtime/output, else
  > deterministic first-by-name; select ``reps_per_class`` where possible,
  > ideally spanning >=2 repos per class.

Runtime/output size isn't in the inventory yet (a future enrichment),
so step 2 collapses into step 3 for v1. The selection that lands picks:

  1. **Tagged-preferred** first (per the manifest's ``selection.prefer_tag``).
  2. **One per repo** across the class — fanning across repos before
     stacking multiples in one — until ``reps_per_class`` is reached.
  3. Then fill remaining slots with second-picks from already-seen repos.

Runnability rules — three buckets per recipe item 5:

  * ``RUNNABLE`` — the default.
  * ``SYNTHETIC_REQUIRED`` — opt-in via the asset tag
    ``synthetic_required: "true"``. We don't try to infer this from FQN
    families (e.g. "snowflake means SYNTHETIC_REQUIRED") because it's
    deployment-specific: some orgs have staging Snowflake, others don't.
  * ``OBSERVE_ONLY`` — opt-in via the asset tag ``observe_only: "true"``,
    OR forced by ``is_executable=False``.

"Tags are the contract" is a deliberate choice for the first two: it's
honest about operator intent, and inference from FQN families can be
layered on later without breaking it. Executability is the one thing that
is NOT a matter of intent. An external/source asset — a bare ``AssetSpec``,
or the stub Dagster auto-creates for a ``deps=[AssetKey(...)]`` naming a
key it doesn't define — has no compute to run, and selecting one is
rejected before the run starts::

    DagsterInvalidDefinitionError: Selected keys must be a subset of
    existing executable asset keys. Invalid selected keys:
    {AssetKey(['<source>', '<schema>', '<table>'])}

Every dlt/Sling-style ingest produces these for the tables it reads from,
and nothing about their tags, group, or key shape distinguishes them from
real assets. Defaulting them to RUNNABLE turned each one into a launch
failure the operator had to triage by hand, so executability is checked
first and outranks the tags.
"""
from __future__ import annotations

from typing import Callable, List, Tuple

from .key import ClassMember, Representative, Runnability


def pick_representatives(
    members: List[ClassMember],
    *,
    prefer_tag: str,
    reps_per_class: int,
) -> List[Representative]:
    """Pick up to ``reps_per_class`` representatives from a class's members.

    Returns a sorted, deterministic list. Selection rules in the module
    docstring.
    """
    if not members or reps_per_class <= 0:
        return []

    # Sort: preferred-tagged first, then repo, then asset_key. This drives
    # both pass 1 (one-per-repo) and pass 2 (fill-from-already-seen-repos).
    sorted_members = sorted(
        members,
        key=lambda m: (
            not _is_preferred(m, prefer_tag),  # False < True -> preferred first
            m.repo,
            m.asset_key,
        ),
    )

    chosen: List[ClassMember] = []
    seen_repos: set = set()

    # Pass 1: span repos. One member per repo, preferred-first.
    for m in sorted_members:
        if len(chosen) >= reps_per_class:
            break
        if m.repo in seen_repos:
            continue
        chosen.append(m)
        seen_repos.add(m.repo)

    # Pass 2: fill remaining slots from already-seen repos, still
    # respecting the preferred-first ordering.
    if len(chosen) < reps_per_class:
        chosen_ids = {id(m) for m in chosen}
        for m in sorted_members:
            if len(chosen) >= reps_per_class:
                break
            if id(m) in chosen_ids:
                continue
            chosen.append(m)

    return [
        _to_representative(m, prefer_tag=prefer_tag)
        for m in chosen
    ]


def classify_runnability(member: ClassMember) -> Tuple[Runnability, str]:
    """Decide the runnability bucket for one member based on its tags.

    Returns ``(Runnability, reason)`` — the reason string is persisted
    alongside the bucket so operators (and tests) can see *why* a class
    landed where it did.
    """
    # Executability outranks every tag: an external asset cannot be launched
    # no matter what an operator asked for, so honoring a tag here would only
    # produce a guaranteed launch failure.
    if member.is_executable is False:
        return (
            Runnability.OBSERVE_ONLY,
            "not executable (external/source asset — Dagster cannot materialize it)",
        )

    tags = member.tags or {}
    if _truthy(tags.get("synthetic_required")):
        return Runnability.SYNTHETIC_REQUIRED, "tag synthetic_required=true"
    if _truthy(tags.get("observe_only")):
        return Runnability.OBSERVE_ONLY, "tag observe_only=true"
    return Runnability.RUNNABLE, "default (no opt-out tag)"


def _to_representative(member: ClassMember, *, prefer_tag: str) -> Representative:
    runnability, reason = classify_runnability(member)
    return Representative(
        repo=member.repo,
        git_sha=member.git_sha,
        asset_key=member.asset_key,
        runnability=runnability,
        runnability_reason=reason,
        is_preferred=_is_preferred(member, prefer_tag),
    )


def _is_preferred(member: ClassMember, prefer_tag: str) -> bool:
    return _truthy((member.tags or {}).get(prefer_tag))


def _truthy(v: object) -> bool:
    return str(v).strip().lower() == "true"

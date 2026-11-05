"""Staging-need classification for Q1 representatives (Phase B1).

Q2 launches one representative alone and does not materialize its
upstreams. Dagster treats an unmaterialized upstream as a *loaded input*
whose output must already exist in the IO manager, so a representative with
a *computed* upstream dies at input loading. This module reports, per
representative, whether that is going to happen. It is reporting only: it
gates nothing and does not touch :class:`Runnability`.

Honesty note: ``NONE_NEEDED`` means *Dagster has no unmaterialized computed
upstream to load*. An external/source upstream still has to exist in
whatever system backs it, which the test deployment's resources decide. So
``NONE_NEEDED`` is "no staged Dagster output required", not "no setup of
any kind required".

The load-bearing rule: ``UNKNOWN`` never collapses into ``NONE_NEEDED``.
``upstream_asset_keys is None`` (a pre-v3 inventory) is unknown, not "no
upstreams"; reporting "nothing needs staging" for an un-resurveyed
inventory would be a vacuous pass.
"""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from dag_tools.inventory import AssetRecord

from .key import ClassMember, StagingNeed

__all__ = [
    "StagingNeed",
    "build_executability_index",
    "classify_staging_need",
]


def build_executability_index(
    records: Iterable[AssetRecord],
) -> Dict[Tuple[str, ...], Optional[bool]]:
    """Map ``tuple(asset_key)`` to whether Dagster can compute that key.

    When several pinned repos describe the same key (repo A holds the
    non-executable stub, repo B defines the real asset) the result is:

    1. any record executable -> ``True``
    2. else any record ``is_executable is None`` -> ``None``
    3. else -> ``False``

    Rule 1 is deliberately conservative and matches the class matrix's
    "can over-segregate but never under-segregates" stance: if *anything*
    in the pinned fleet computes that key, its output will not exist when
    Q2 launches one asset alone, so staging is required.
    """
    seen: Dict[Tuple[str, ...], List[Optional[bool]]] = {}
    for rec in records:
        seen.setdefault(tuple(rec.asset_key), []).append(rec.is_executable)
    index: Dict[Tuple[str, ...], Optional[bool]] = {}
    for key, flags in seen.items():
        if any(f is True for f in flags):
            index[key] = True
        elif any(f is None for f in flags):
            index[key] = None
        else:
            index[key] = False
    return index


def classify_staging_need(
    member: ClassMember,
    index: Dict[Tuple[str, ...], Optional[bool]],
) -> Tuple[StagingNeed, str]:
    """Decide the staging need for one member; returns ``(need, reason)``.

    ``REQUIRED`` wins over ``UNKNOWN`` when both apply: a known-needed
    staging is the more actionable fact.
    """
    ups = member.upstream_asset_keys
    if ups is None:
        return (
            StagingNeed.UNKNOWN,
            "inventory predates upstream_asset_keys (schema_version < 3); "
            "re-survey the pinned SHA to classify it",
        )
    if not ups:
        return StagingNeed.NONE_NEEDED, "no declared upstreams"

    computed: List[str] = []
    unresolved: List[str] = []
    unknown_exec: List[str] = []
    for up in ups:
        name = "/".join(up)
        if tuple(up) not in index:
            unresolved.append(name)
            continue
        flag = index[tuple(up)]
        if flag is None:
            unknown_exec.append(name)
        elif flag:
            computed.append(name)

    if computed:
        return (
            StagingNeed.REQUIRED,
            "computed upstream(s) Q2 will not materialize: "
            + ", ".join(computed),
        )
    if unresolved:
        return (
            StagingNeed.UNKNOWN,
            "upstream not found in pinned inventories: " + ", ".join(unresolved),
        )
    if unknown_exec:
        return (
            StagingNeed.UNKNOWN,
            "upstream with unknown executability: " + ", ".join(unknown_exec),
        )
    return (
        StagingNeed.NONE_NEEDED,
        f"all {len(ups)} upstream(s) are external/source assets",
    )

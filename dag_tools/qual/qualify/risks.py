"""Compute co_upgrade_risks by diffing baseline and candidate pin sets.

Recipe rationale (Phase Q0, item 3):

  > Diffs resolved transitive pins (notably dbt-core, dbt adapters,
  > warehouse clients) between baseline and candidate and records
  > co_upgrade_risks[] — a dbt-core bump hidden inside a Dagster bump
  > must be called out, not discovered later as a false Dagster regression.

So a "risk" is **a non-Dagster library whose version differs between
baseline and candidate pins**. The Dagster-family libraries (dagster,
dagster-dbt, dagster-k8s, dagster-aws, etc.) are the *explicit* upgrade
target and are filtered out — only hidden co-upgrades get flagged.

Severity heuristic:

  * ``"major"`` when the major version component differs (e.g. ``1.x ->
    2.x``). These are the most likely false-positives for "Dagster broke
    my pipeline" and should be separately validated or pinned back.
  * ``"warning"`` for minor/patch changes — record but lower-urgency.
  * Wildcards (``"1.10.x"``) treat the ``x`` as "any" — a ``"1.10.x" ->
    "1.10.5"`` diff is NOT flagged (same major.minor); ``"1.10.x" ->
    "1.11.0"`` IS flagged as a minor warning.

Strict semver is not assumed — operators sometimes pin to non-semver
strings (release tags, git SHAs). Anything we can't parse is reported as
``"warning"`` with the raw strings carried through.
"""
from __future__ import annotations

import re
from typing import Dict, List, Tuple

from .manifest import CoUpgradeRisk


# Dagster-family libraries — these are the *target* of the upgrade, not
# "hidden co-upgrade risks". Anything matching this prefix gets filtered.
DAGSTER_FAMILY_PREFIX = "dagster"


def compute_co_upgrade_risks(
    baseline_pins: Dict[str, str],
    candidate_pins: Dict[str, str],
    *,
    dagster_family_prefixes: Tuple[str, ...] = (DAGSTER_FAMILY_PREFIX,),
) -> List[CoUpgradeRisk]:
    """Diff two pin sets and return the non-Dagster libraries whose
    versions differ.

    Args:
      baseline_pins: ``{lib_name: version_str}`` for the baseline side.
      candidate_pins: same shape, for the candidate side.
      dagster_family_prefixes: name prefixes treated as the explicit
        upgrade target and excluded from the diff. Default
        ``("dagster",)`` catches ``dagster``, ``dagster-dbt``,
        ``dagster-k8s``, etc.

    Returns:
      A list of ``CoUpgradeRisk``, sorted by ``(severity desc, lib asc)``
      so majors float to the top.
    """
    risks: List[CoUpgradeRisk] = []

    shared = set(baseline_pins) & set(candidate_pins)
    for lib in sorted(shared):
        if any(lib.startswith(p) for p in dagster_family_prefixes):
            continue
        base = baseline_pins[lib]
        cand = candidate_pins[lib]
        if _versions_equivalent(base, cand):
            continue
        risks.append(CoUpgradeRisk(
            lib=lib,
            **{"from": base, "to": cand},
            severity=_severity(base, cand),
        ))

    # majors first (more urgent), then by name for stability.
    severity_rank = {"major": 0, "warning": 1}
    risks.sort(key=lambda r: (severity_rank.get(r.severity, 99), r.lib))
    return risks


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


_VERSION_PART_RE = re.compile(r"^[0-9]+$")


def _parse_version(v: str) -> List[str]:
    """Split a version string into components: ``"1.10.5"`` -> ``["1","10","5"]``.

    Pre-release suffixes (``"1.10.5rc1"``) and wildcards (``"1.10.x"``) are
    preserved as strings — comparison logic handles them.
    """
    # Drop pre-release tail at the first non-version separator.
    head = re.split(r"[+]", v, 1)[0]
    parts = head.split(".")
    return parts


def _versions_equivalent(a: str, b: str) -> bool:
    """Two versions are equivalent when their components match, treating
    ``x`` as a wildcard.

    Examples (equivalent):
      ``"1.10.5"`` vs ``"1.10.5"``
      ``"1.10.x"`` vs ``"1.10.5"``
      ``"1.x"`` vs ``"1.10.5"``

    Examples (NOT equivalent):
      ``"1.10.5"`` vs ``"1.10.6"``
      ``"1.10.x"`` vs ``"1.11.0"``
    """
    if a == b:
        return True
    pa, pb = _parse_version(a), _parse_version(b)
    for x, y in zip(pa, pb):
        if x == "x" or y == "x":
            continue
        if x != y:
            return False
    # If lengths differ ('1.10' vs '1.10.5') and the extra parts aren't 'x',
    # they're not equivalent unless the longer side's extras are zero.
    longer = pa if len(pa) > len(pb) else pb
    extras = longer[min(len(pa), len(pb)):]
    return all(e in ("0", "x") for e in extras)


def _severity(base: str, cand: str) -> str:
    """Major version bumps are ``"major"``; minor/patch are ``"warning"``.

    A wildcard major (``"x.y.z"``) is treated as warning since the
    comparison is ambiguous.
    """
    pa = _parse_version(base)
    pb = _parse_version(cand)
    if not pa or not pb:
        return "warning"
    head_a, head_b = pa[0], pb[0]
    if head_a == "x" or head_b == "x":
        return "warning"
    if not _VERSION_PART_RE.match(head_a) or not _VERSION_PART_RE.match(head_b):
        return "warning"
    return "major" if head_a != head_b else "warning"

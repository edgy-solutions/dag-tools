"""High-level Q1 orchestrator: read manifest -> fetch inventories ->
build classes -> render artifacts -> publish.

This module reads the qualification manifest's ``inventory_pins[]`` (NOT
the registry's current ``latest.json``). That's the load-bearing
invariant from AGENTS.md §4d: later Q-phases MUST honor the pinned
snapshot or qualification reproducibility breaks silently.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import yaml
from collections import Counter, defaultdict

from dag_tools.inventory import AssetRecord

from ..qualify import QualificationManifest, Selection
from ..registry import InventoryRegistry, layout
from .key import (
    ClassKeyComponents,
    ClassMatrix,
    ClassMember,
    EquivalenceClass,
    Runnability,
    class_hash,
    compute_class_key,
)
from .selection import pick_representatives


logger = logging.getLogger(__name__)


def build_class_matrix(
    qual_id: str,
    *,
    registry: InventoryRegistry,
    now: Optional[datetime] = None,
) -> ClassMatrix:
    """Read the manifest, fetch each pinned build's inventory, compute
    equivalence classes, pick representatives, and return the matrix.

    Does **not** publish — callers (typically :func:`publish_class_matrix`
    or the CLI) own the registry write so they can apply ``allow_overwrite``.
    """
    now = now or datetime.now(tz=timezone.utc)
    manifest = _read_manifest(registry, qual_id)
    selection = manifest.selection

    # 1. Fetch every pinned build's assets + dbt projects.
    members: List[ClassMember] = []
    key_components: Dict[Tuple[str, ...], ClassKeyComponents] = {}
    class_member_map: Dict[str, List[ClassMember]] = defaultdict(list)

    for pin in manifest.inventory_pins:
        records = _read_asset_records(registry, pin.repo, pin.git_sha)
        custom_translators = _custom_dbt_translators(registry, pin.repo, pin.git_sha)
        for record in records:
            member = ClassMember(
                repo=pin.repo,
                git_sha=pin.git_sha,
                asset_key=record.asset_key,
                location=record.location,
                tags=dict(record.tags or {}),
            )
            components = compute_class_key(
                record, custom_dbt_translators_in_repo=custom_translators
            )
            ch = class_hash(components)
            class_member_map[ch].append(member)
            key_components.setdefault(_key_components_id(ch), components)
            members.append(member)

    # 2. Assemble per-class records with representatives.
    classes: List[EquivalenceClass] = []
    for ch in sorted(class_member_map.keys()):
        cls_members = class_member_map[ch]
        repos = sorted({m.repo for m in cls_members})
        reps = pick_representatives(
            cls_members,
            prefer_tag=selection.prefer_tag,
            reps_per_class=selection.reps_per_class,
        )
        classes.append(EquivalenceClass(
            class_hash=ch,
            key=key_components[_key_components_id(ch)],
            member_count=len(cls_members),
            member_repo_count=len(repos),
            members=sorted(
                cls_members,
                key=lambda m: (m.repo, m.asset_key),
            ),
            representatives=reps,
        ))

    # 3. Coverage roll-up — over the reps, not the members.
    coverage = Counter(
        rep.runnability.value
        for cls in classes
        for rep in cls.representatives
    )
    for r in Runnability:
        coverage.setdefault(r.value, 0)

    return ClassMatrix(
        qual_id=qual_id,
        generated_at=now,
        asset_count=len(members),
        class_count=len(classes),
        coverage_by_runnability=dict(coverage),
        classes=classes,
    )


def publish_class_matrix(
    matrix: ClassMatrix,
    *,
    registry: InventoryRegistry,
    allow_overwrite: bool = False,
) -> None:
    """Write the matrix to the registry as both JSON and Markdown."""
    registry.put_qualification_classes(
        qual_id=matrix.qual_id,
        json_body=matrix.model_dump_json(indent=2).encode("utf-8"),
        markdown_body=render_markdown(matrix).encode("utf-8"),
        allow_overwrite=allow_overwrite,
    )


# ---------------------------------------------------------------------------
# Markdown rendering — the operator-eyeballing companion to the JSON.
# ---------------------------------------------------------------------------


def render_markdown(matrix: ClassMatrix) -> str:
    """Render the matrix as a Markdown document for human review.

    Per the recipe: "print the coverage table for operator review BEFORE
    anything runs". This is what the operator stares at to decide whether
    the class matrix is sane before launching baseline.
    """
    lines: List[str] = []
    lines.append(f"# Equivalence-class matrix — `{matrix.qual_id}`")
    lines.append("")
    lines.append(f"Generated at {matrix.generated_at.isoformat()}.")
    lines.append("")
    lines.append("## Coverage")
    lines.append("")
    lines.append(f"- Total assets: **{matrix.asset_count}**")
    lines.append(f"- Equivalence classes: **{matrix.class_count}**")
    lines.append("- Representatives by runnability:")
    for runnab in Runnability:
        n = matrix.coverage_by_runnability.get(runnab.value, 0)
        lines.append(f"  - **{runnab.value}**: {n}")
    lines.append("")

    lines.append("## Classes")
    lines.append("")
    lines.append("| Class hash | Size | Repos | Reps | Compute | IO manager | Integrations | Notes |")
    lines.append("|---|---:|---:|---:|---|---|---|---|")
    for cls in matrix.classes:
        notes_bits = []
        if cls.key.custom_dbt_translator_classes:
            notes_bits.append("custom-dbt-translator")
        if cls.key.has_asset_checks:
            notes_bits.append("checks")
        if cls.key.automation_condition_type:
            notes_bits.append("automation")
        notes = ", ".join(notes_bits) or "—"
        io_class = cls.key.io_manager_class or "—"
        compute = cls.key.compute_kind or "—"
        integrations = ", ".join(cls.key.integration_libs) or "—"
        lines.append(
            f"| `{cls.class_hash}` "
            f"| {cls.member_count} "
            f"| {cls.member_repo_count} "
            f"| {len(cls.representatives)} "
            f"| {compute} "
            f"| `{_short_fqn(io_class)}` "
            f"| {integrations} "
            f"| {notes} |"
        )
    lines.append("")

    # Per-class detail. Keep tight; operators scan rather than read.
    for cls in matrix.classes:
        lines.append(f"### `{cls.class_hash}`")
        lines.append("")
        lines.append(
            f"**{cls.member_count}** asset(s) across "
            f"**{cls.member_repo_count}** repo(s)."
        )
        lines.append("")
        if cls.representatives:
            lines.append("Representatives:")
            for rep in cls.representatives:
                ak = "/".join(rep.asset_key)
                pref = " ⭐" if rep.is_preferred else ""
                lines.append(
                    f"- **{rep.runnability.value}**: `{rep.repo}@{rep.git_sha[:12]}` "
                    f"→ `{ak}`{pref}  _({rep.runnability_reason})_"
                )
            lines.append("")
        # First few members (in addition to reps) so the operator sees what's in the bucket.
        if cls.member_count > len(cls.representatives):
            lines.append("Members (sample):")
            sample = [m for m in cls.members if m not in [
                ClassMember(**r.model_dump(include={"repo", "git_sha", "asset_key"}))
                for r in cls.representatives
            ]][:5]
            for m in sample:
                ak = "/".join(m.asset_key)
                lines.append(f"  - `{m.repo}` `{ak}`")
            if cls.member_count - len(cls.representatives) - len(sample) > 0:
                more = cls.member_count - len(cls.representatives) - len(sample)
                lines.append(f"  - …and {more} more")
            lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal: registry fetch + parse
# ---------------------------------------------------------------------------


def _read_manifest(registry: InventoryRegistry, qual_id: str) -> QualificationManifest:
    body = registry.read_qualification_manifest(qual_id)
    if body is None:
        raise FileNotFoundError(
            f"no qualification manifest at qual_id={qual_id!r}; "
            f"run `dagtools qual init --id {qual_id} ...` first"
        )
    doc = yaml.safe_load(body)
    return QualificationManifest.model_validate(doc)


def _read_asset_records(
    registry: InventoryRegistry, repo: str, sha: str
) -> List[AssetRecord]:
    doc = registry.read_build_json(repo, sha, layout.ASSETS_FILE)
    if doc is None:
        logger.warning(
            "_read_asset_records: repo %r SHA %r has no assets.json — skipping",
            repo, sha,
        )
        return []
    records = doc.get("records", [])
    out: List[AssetRecord] = []
    for r in records:
        try:
            out.append(AssetRecord.model_validate(r))
        except Exception as e:
            logger.warning(
                "_read_asset_records: failed to validate record in %r@%r: %s",
                repo, sha, e,
            )
    return out


def _custom_dbt_translators(
    registry: InventoryRegistry, repo: str, sha: str
) -> List[str]:
    """Return FQNs of every custom dbt translator detected in this repo.

    Empty when the repo has no dbt projects, no custom translators, or
    the inventory pre-dates the dbt-projects artifact.
    """
    doc = registry.read_build_json(repo, sha, layout.DBT_PROJECTS_FILE)
    if not doc:
        return []
    projects = doc.get("projects") or []
    return sorted({
        p.get("translator_class")
        for p in projects
        if p.get("is_custom_translator") and p.get("translator_class")
    })


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------


def _key_components_id(class_hash_str: str) -> Tuple[str, ...]:
    """Wraps the class_hash in a singleton tuple so the registry dict
    accepts it as a key. Kept as a helper in case we want to evolve the
    component-identity story (e.g. multi-key dedupe) later."""
    return (class_hash_str,)


def _short_fqn(fqn: str) -> str:
    """Render the tail two segments of an FQN for the markdown table.

    ``dagster._core.storage.mem_io_manager.InMemoryIOManager`` ->
    ``mem_io_manager.InMemoryIOManager``. Keeps the table from blowing
    out horizontally without losing the distinguishing tail.
    """
    if not fqn or fqn == "—":
        return fqn
    parts = fqn.rsplit(".", 2)
    return ".".join(parts[-2:]) if len(parts) >= 2 else fqn

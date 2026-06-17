"""``dagtools qual init`` orchestrator.

Builds a :class:`QualificationManifest` from operator inputs + the current
registry state, then writes it to two places:

  1. The registry at ``qualifications/<qual_id>/manifest.yaml`` (durable,
     shared, immutable). This is the canonical copy every later phase reads.
  2. A local file (default ``~/.dagtools/quals/<qual_id>/manifest.yaml``)
     for operator convenience — fast inspection, edit-then-re-init flow
     during early iteration.

The registry write uses :meth:`InventoryRegistry.put_qualification_manifest`,
which refuses to overwrite by default. ``--allow-overwrite`` exists for the
"I really do want to redo this qualification" path.

Recipe rule (Phase Q0):

  > Pulls ``latest.json`` for every repo under ``inventory/``; **pins** the
  > exact ``(repo, git_sha)`` set into the manifest. The qualification is
  > now immune to builds that land mid-qualification.

So this module reads the registry **once** at init time and freezes that
snapshot. Subsequent ``dagtools survey`` publishes do not affect this
qualification.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from ..registry import InventoryRegistry
from .manifest import (
    Deployment,
    InventoryPin,
    QualificationManifest,
    Selection,
    VersionTarget,
)
from .risks import compute_co_upgrade_risks


logger = logging.getLogger(__name__)


def default_local_manifest_path(qual_id: str) -> Path:
    """``~/.dagtools/quals/<qual_id>/manifest.yaml`` — the convention used
    everywhere we need a per-operator scratch space (also where the Q4
    run-state file will live)."""
    home = Path(os.environ.get("DAGTOOLS_HOME") or (Path.home() / ".dagtools"))
    return home / "quals" / qual_id / "manifest.yaml"


def create_qualification(
    qual_id: str,
    *,
    registry: InventoryRegistry,
    baseline: VersionTarget,
    candidate: VersionTarget,
    deployment: Optional[Deployment] = None,
    staging_overrides: Optional[str] = None,
    selection: Optional[Selection] = None,
    local_path: Optional[Path] = None,
    allow_overwrite: bool = False,
    now: Optional[datetime] = None,
) -> QualificationManifest:
    """Pin the registry snapshot, build the manifest, write it locally + remotely.

    Returns the resulting manifest. Raises through any registry-level
    error (e.g. ``ImmutableKeyExists`` when ``allow_overwrite=False`` and a
    manifest already exists).
    """
    now = now or datetime.now(tz=timezone.utc)
    deployment = deployment or Deployment()
    selection = selection or Selection()
    local_path = local_path or default_local_manifest_path(qual_id)

    inventory_pins = _pin_inventories(registry)
    co_upgrade_risks = compute_co_upgrade_risks(baseline.pins, candidate.pins)

    manifest = QualificationManifest(
        qual_id=qual_id,
        created_at=now,
        baseline=baseline,
        candidate=candidate,
        co_upgrade_risks=co_upgrade_risks,
        inventory_pins=inventory_pins,
        deployment=deployment,
        staging_overrides=staging_overrides,
        selection=selection,
    )

    yaml_body = _to_yaml_bytes(manifest)

    # Registry first — failures (e.g. ImmutableKeyExists) abort BEFORE we
    # write anything locally, keeping operator state consistent with the
    # registry's view of "this qual exists".
    registry.put_qualification_manifest(
        qual_id, yaml_body, allow_overwrite=allow_overwrite,
    )

    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(yaml_body)
    logger.info(
        "create_qualification: wrote manifest for %s to registry + %s",
        qual_id, local_path,
    )

    return manifest


def _pin_inventories(registry: InventoryRegistry) -> List[InventoryPin]:
    """Walk every repo under ``inventory/``, read ``latest.json``, freeze
    into a list of ``InventoryPin``.

    Repos that have a registry prefix but no ``latest.json`` are skipped
    with a WARNING — they are unhealthy state we don't want to include
    silently. (``dagtools registry status`` already surfaces them.)
    """
    pins: List[InventoryPin] = []
    for repo in registry.list_repos():
        pointer = registry.read_latest_pointer(repo)
        if pointer is None:
            logger.warning(
                "_pin_inventories: repo %r has no latest.json — skipping",
                repo,
            )
            continue
        pins.append(InventoryPin(
            repo=repo,
            git_sha=pointer.git_sha,
            pinned_timestamp=pointer.timestamp,
        ))
    return pins


def _to_yaml_bytes(manifest: QualificationManifest) -> bytes:
    """Serialize manifest to YAML bytes. Uses ``model_dump(mode='json')``
    to get ISO timestamps + plain scalars, then dumps via PyYAML.

    ``co_upgrade_risk`` aliases ``from`` / ``to`` need ``by_alias=True``
    so the YAML matches the recipe's sample shape.
    """
    payload = manifest.model_dump(mode="json", by_alias=True)
    return yaml.safe_dump(payload, sort_keys=False, default_flow_style=False).encode("utf-8")

"""Orchestrator: read class matrix → generate probes for every
SYNTHETIC_REQUIRED class → publish to registry + write locally.

Local path mirrors the rest of the qualification system's operator-state
convention: ``~/.dagtools/quals/<qual_id>/probes/<class_hash>.py``. This
is where the operator picks up the generated probes to drop into the
``dag-tools-probes`` user-code location.

Public:
  * :func:`generate_bundle` — read + generate, return :class:`ProbeManifest`.
  * :func:`publish_bundle` — push every source + the manifest to the registry.
  * :func:`write_local_bundle` — write the operator's local copy.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import yaml

from ..classes import ClassMatrix, Runnability
from ..qualify import QualificationManifest
from ..registry import InventoryRegistry
from .generator import generate_probe_module
from .schema import ProbeManifest, ProbeModule


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------


def generate_bundle(
    qual_id: str,
    *,
    registry: InventoryRegistry,
    now: Optional[datetime] = None,
) -> "GeneratedBundle":
    """Read the manifest + class matrix, generate a probe per
    SYNTHETIC_REQUIRED class, and return the manifest + the source map.

    The returned :class:`GeneratedBundle` carries the source strings so
    :func:`publish_bundle` and :func:`write_local_bundle` can dispatch
    them without re-generating.
    """
    now = now or datetime.now(tz=timezone.utc)

    _read_manifest(registry, qual_id)  # raises FileNotFoundError if missing
    matrix = _read_class_matrix(registry, qual_id)

    probes: List[ProbeModule] = []
    sources: dict[str, str] = {}
    skipped: List[str] = []

    synthetic_total = 0
    for cls in matrix.classes:
        if not cls.representatives:
            continue
        if cls.representatives[0].runnability != Runnability.SYNTHETIC_REQUIRED:
            continue
        synthetic_total += 1
        probe, source_or_reason = generate_probe_module(cls, qual_id=qual_id)
        if probe is None:
            logger.warning(
                "generate_bundle: class %s skipped: %s",
                cls.class_hash, source_or_reason,
            )
            skipped.append(cls.class_hash)
            continue
        probes.append(probe)
        sources[cls.class_hash] = source_or_reason

    return GeneratedBundle(
        manifest=ProbeManifest(
            qual_id=qual_id,
            generated_at=now,
            synthetic_class_count=synthetic_total,
            probes=probes,
            skipped_class_hashes=skipped,
        ),
        sources=sources,
    )


class GeneratedBundle:
    """In-memory bundle returned by :func:`generate_bundle`.

    Holds the manifest plus the ``class_hash -> source`` map so publishers
    don't re-derive the source text. Not a pydantic model — pure transport."""

    def __init__(self, manifest: ProbeManifest, sources: dict[str, str]):
        self.manifest = manifest
        self.sources = sources


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------


def publish_bundle(
    bundle: GeneratedBundle,
    *,
    registry: InventoryRegistry,
    allow_overwrite: bool = False,
) -> None:
    """Push every probe source to the registry, then the manifest LAST.

    Mirrors the survey's "pointer-written-last" invariant: a reader that
    sees ``probe_manifest.json`` is guaranteed every referenced
    ``<class_hash>.py`` is present. If a source write fails the manifest
    is never updated.
    """
    for probe in bundle.manifest.probes:
        source = bundle.sources.get(probe.class_hash)
        if source is None:
            raise RuntimeError(
                f"publish_bundle: missing source for class {probe.class_hash!r} "
                "— bundle is internally inconsistent"
            )
        registry.put_probe_source(
            qual_id=bundle.manifest.qual_id,
            class_hash=probe.class_hash,
            body=source.encode("utf-8"),
            allow_overwrite=allow_overwrite,
        )

    registry.put_probe_manifest(
        qual_id=bundle.manifest.qual_id,
        body=bundle.manifest.model_dump_json(indent=2).encode("utf-8"),
        allow_overwrite=allow_overwrite,
    )


def write_local_bundle(
    bundle: GeneratedBundle,
    *,
    local_path: Optional[Path] = None,
) -> Path:
    """Write the generated probes to a local directory for operator pickup.

    Defaults to ``~/.dagtools/quals/<qual_id>/probes/``. Returns the
    directory the bundle was written to so callers can echo it back at
    the operator.
    """
    local_path = local_path or default_local_probes_dir(bundle.manifest.qual_id)
    local_path.mkdir(parents=True, exist_ok=True)
    for probe in bundle.manifest.probes:
        source = bundle.sources.get(probe.class_hash)
        if source is None:
            continue
        (local_path / f"{probe.class_hash}.py").write_text(source, encoding="utf-8")
    (local_path / "probe_manifest.json").write_bytes(
        bundle.manifest.model_dump_json(indent=2).encode("utf-8")
    )
    return local_path


def default_local_probes_dir(qual_id: str) -> Path:
    """``~/.dagtools/quals/<qual_id>/probes/`` — same root the rest of the
    qualification system uses for operator-local state."""
    home = Path(os.environ.get("DAGTOOLS_HOME") or (Path.home() / ".dagtools"))
    return home / "quals" / qual_id / "probes"


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------


def _read_manifest(registry: InventoryRegistry, qual_id: str) -> QualificationManifest:
    body = registry.read_qualification_manifest(qual_id)
    if body is None:
        raise FileNotFoundError(
            f"no qualification manifest at qual_id={qual_id!r}; "
            f"run `dagtools qual init --id {qual_id} ...` first"
        )
    return QualificationManifest.model_validate(yaml.safe_load(body))


def _read_class_matrix(registry: InventoryRegistry, qual_id: str) -> ClassMatrix:
    body = registry.read_qualification_classes_json(qual_id)
    if body is None:
        raise FileNotFoundError(
            f"no equivalence-class matrix for qual_id={qual_id!r}; "
            f"run `dagtools qual classes --id {qual_id}` first"
        )
    return ClassMatrix.model_validate_json(body)

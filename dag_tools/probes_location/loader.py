"""Dynamic loader for the ``dag-tools-probes`` code location.

Reads every ``<class_hash>.py`` under ``DAGTOOLS_PROBES_DIR``, imports
each as a fresh module, harvests its top-level ``defs``, and merges
them via :func:`dagster.Definitions.merge`.

Per-file soft-fail: a probe that raises during import (missing IO
manager FQN with no fallback, syntax error from a manual edit, etc.)
becomes an entry in :class:`ProbeLoadReport.failures` with the
exception captured. The healthy probes still load — operators triage
failures from the report.

Recipe rule: probes are auto-generated artifacts; the location's
deploy cycle must be decoupled from ``dag-tools`` releases, so this
module imports only ``dagster`` + stdlib. Each generated probe imports
its own IO manager FQN with an ``InMemoryIOManager`` fallback so a
missing transitive dependency degrades gracefully instead of failing
the location load.
"""
from __future__ import annotations

import importlib.util
import logging
import os
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


logger = logging.getLogger(__name__)


# The environment variable the operator sets on their test deployment
# to point the code location at the bundle directory. Default falls
# back to the manifest's local-bundle convention.
DAGTOOLS_PROBES_DIR_ENV = "DAGTOOLS_PROBES_DIR"

# Documentation hint surfaced in error messages — NOT the actual
# default resolution (the operator's HOME or DAGTOOLS_HOME shapes it).
DEFAULT_PROBES_DIR_HINT = "~/.dagtools/quals/<qual_id>/probes/"


# ---------------------------------------------------------------------------
# Outcome shapes
# ---------------------------------------------------------------------------


@dataclass
class ProbeLoadOutcome:
    """Per-probe load result. ``defs`` is set on success; ``error`` is
    set on failure. ``class_hash`` is the file basename without ``.py``."""
    class_hash: str
    path: Path
    defs: Optional[object] = None
    error: Optional[str] = None

    @property
    def loaded(self) -> bool:
        return self.error is None and self.defs is not None


@dataclass
class ProbeLoadReport:
    """Aggregate load report for the location.

    ``loaded`` is the list of probes whose ``defs`` were captured;
    ``failures`` is the list of probes that raised during import.
    Surfaced as Dagster metadata on the merged location so operators
    can see which class_hashes are missing — silent truncation of a
    probe directory is the failure mode we will not tolerate.
    """
    probes_dir: Path
    loaded: List[ProbeLoadOutcome] = field(default_factory=list)
    failures: List[ProbeLoadOutcome] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.loaded) + len(self.failures)


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def resolve_probes_dir() -> Optional[Path]:
    """Resolve the probes directory from ``DAGTOOLS_PROBES_DIR``.

    Returns ``None`` when unset — the loader treats that as "no probes
    deployed yet" rather than an error, so the location loads cleanly
    on a test deployment that doesn't yet have a bundle.
    """
    raw = os.environ.get(DAGTOOLS_PROBES_DIR_ENV)
    if not raw:
        return None
    return Path(raw).expanduser()


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def load_probes_from_dir(probes_dir: Optional[Path] = None) -> ProbeLoadReport:
    """Load every ``<class_hash>.py`` under ``probes_dir`` and report.

    Returns a :class:`ProbeLoadReport` even on partial / total failure.
    Caller composes the merged ``Definitions`` from
    ``[outcome.defs for outcome in report.loaded]``.

    Skips ``probe_manifest.json`` and any non-``.py`` files. Files that
    fail to import are recorded with the captured exception text but
    never raise.
    """
    probes_dir = probes_dir or resolve_probes_dir()
    if probes_dir is None:
        logger.info(
            "dag-tools-probes: %s unset; loading empty location (hint: %s)",
            DAGTOOLS_PROBES_DIR_ENV, DEFAULT_PROBES_DIR_HINT,
        )
        return ProbeLoadReport(probes_dir=Path(DEFAULT_PROBES_DIR_HINT))

    if not probes_dir.is_dir():
        logger.warning(
            "dag-tools-probes: %s=%s does not exist or is not a directory; "
            "loading empty location", DAGTOOLS_PROBES_DIR_ENV, probes_dir,
        )
        return ProbeLoadReport(probes_dir=probes_dir)

    report = ProbeLoadReport(probes_dir=probes_dir)
    for path in sorted(probes_dir.glob("*.py")):
        class_hash = path.stem
        outcome = _load_one(class_hash, path)
        if outcome.loaded:
            report.loaded.append(outcome)
        else:
            report.failures.append(outcome)
            logger.warning(
                "dag-tools-probes: probe %s failed to load: %s",
                class_hash, outcome.error,
            )

    logger.info(
        "dag-tools-probes: loaded %d probe(s) from %s, %d failure(s)",
        len(report.loaded), probes_dir, len(report.failures),
    )
    return report


def _load_one(class_hash: str, path: Path) -> ProbeLoadOutcome:
    """Import ``path`` as a fresh module under a stable synthetic name
    (``dag_tools_probes_dyn.<class_hash>``) and pull its ``defs``.

    The synthetic-module-name pattern keeps repeated loads idempotent
    inside the same process — important if Dagster reloads the
    location and we'd otherwise re-execute the probe module multiple
    times under different names.
    """
    module_name = f"dag_tools_probes_dyn.{class_hash}"
    try:
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            return ProbeLoadOutcome(
                class_hash=class_hash, path=path,
                error=f"spec_from_file_location returned None for {path!r}",
            )
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
    except Exception as exc:
        return ProbeLoadOutcome(
            class_hash=class_hash, path=path,
            error=f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
        )

    defs = getattr(module, "defs", None)
    if defs is None:
        return ProbeLoadOutcome(
            class_hash=class_hash, path=path,
            error=(
                f"module {path!r} loaded but exposes no top-level `defs` "
                "Definitions; not a dag-tools-probes module"
            ),
        )
    return ProbeLoadOutcome(class_hash=class_hash, path=path, defs=defs)

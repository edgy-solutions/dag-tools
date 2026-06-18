"""The ``dag-tools-probes`` Dagster code location.

This is the deploy target for the modules ``dagtools qual synthetic``
generates. The operator points their **test deployment's** workspace at
``dag_tools.probes_location.definitions`` (typically as a separate user-
code location) and sets ``DAGTOOLS_PROBES_DIR`` to the directory the
probe bundle was written to — e.g. ``~/.dagtools/quals/<qual_id>/probes/``.

The location dynamically loads every ``<class_hash>.py`` in that
directory, harvests each module's top-level ``defs`` (a ``Definitions``
emitted by the Q5 generator), and merges them into a single ``defs``
the Dagster deployment exposes.

**Soft-fail per probe.** One broken probe (a missing IO manager FQN,
a syntax error from a corrupted file, etc.) MUST NOT block the whole
location — operators triage the failures in the load report instead.
This mirrors the survey's per-asset extraction discipline.

Public:
  * :func:`load_probes_from_dir` — the loader (testable independently).
  * :data:`defs` (in :mod:`.definitions`) — the top-level ``Definitions``
    the deployment imports.
"""
from .loader import (
    DAGTOOLS_PROBES_DIR_ENV,
    DEFAULT_PROBES_DIR_HINT,
    ProbeLoadOutcome,
    ProbeLoadReport,
    load_probes_from_dir,
    resolve_probes_dir,
)

__all__ = [
    "DAGTOOLS_PROBES_DIR_ENV",
    "DEFAULT_PROBES_DIR_HINT",
    "ProbeLoadOutcome",
    "ProbeLoadReport",
    "load_probes_from_dir",
    "resolve_probes_dir",
]

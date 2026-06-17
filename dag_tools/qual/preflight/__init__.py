"""Phase 2 Q3 — test-deployment preflight before/after the version bump.

Public surface:
  * :func:`run_preflight` — runs the three checks, returns a
    :class:`PreflightReport`.
  * :func:`publish_preflight_report` — persist to the registry.
  * :class:`PreflightReport`, :class:`CheckResult`, :class:`CodeLocationCheck`,
    :class:`RunRenderingCheck` — the persisted shapes.
"""
from .preflight import (
    SCHEMA_VERSION,
    CheckResult,
    CodeLocationCheck,
    PreflightReport,
    RunRenderingCheck,
    publish_preflight_report,
    run_preflight,
)

__all__ = [
    "SCHEMA_VERSION",
    "CheckResult",
    "CodeLocationCheck",
    "PreflightReport",
    "RunRenderingCheck",
    "publish_preflight_report",
    "run_preflight",
]

"""Phase 2 Q6 — diff baseline vs candidate and emit the operator verdict.

Public surface:
  * :func:`build_verdict` — read all qual_id artifacts, return Verdict.
  * :func:`publish_verdict` — write verdict.json + UPGRADE_VERDICT.md.
  * :func:`render_markdown` — operator-facing companion.
  * :class:`Verdict`, :class:`VerdictStatus`, :class:`GapAcceptance` — verdict shape.
  * :class:`RepDiff`, :class:`ClassVerdict`, :func:`diff_rep`,
    :func:`build_class_verdicts` — per-rep / per-class building blocks.
"""
from .diff import ClassVerdict, RepDiff, build_class_verdicts, diff_rep
from .verdict import (
    GapAcceptance,
    SCHEMA_VERSION,
    Verdict,
    VerdictStatus,
    build_verdict,
    publish_verdict,
    render_markdown,
)

__all__ = [
    "SCHEMA_VERSION",
    # verdict
    "Verdict",
    "VerdictStatus",
    "GapAcceptance",
    "build_verdict",
    "publish_verdict",
    "render_markdown",
    # diff
    "RepDiff",
    "ClassVerdict",
    "diff_rep",
    "build_class_verdicts",
]

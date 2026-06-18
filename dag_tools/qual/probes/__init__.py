"""Phase 2 Q5c — probe runner.

For every probe in the qual's probe manifest, launch its DOWNSTREAM
asset (deps pulls the upstream automatically) through the test
deployment's ``dag-tools-probes`` user-code location, poll to a
terminal state, persist a per-probe ``RunRecord`` (reused from the
Q2 runs subpackage), and mirror the per-side probe state to the
registry. Resumability + LAUNCHED-reconciliation are the same as Q2.

Output: a ``ProbeSideSummary`` per ``(qual_id, side)`` that Q6 reads
to count synthetic classes with passing probes as covered.

Public:
  * :func:`run_probes_side` — orchestrator.
  * :class:`ProbeRunState` / :class:`ProbeRepState` / :class:`ProbeRepStatus`
    — persisted per-side state schemas.
  * :class:`ProbeSideOutcome` / :class:`ProbeSideSummary` — invocation
    return.
"""
from .runner import (
    ProbeSideOutcome,
    ProbeSideSummary,
    run_probes_side,
)
from .state import (
    SCHEMA_VERSION as SCHEMA_VERSION_PROBES_STATE,
    ProbeRepState,
    ProbeRepStatus,
    ProbeRunState,
    default_local_probes_state_path,
)

__all__ = [
    "SCHEMA_VERSION_PROBES_STATE",
    "ProbeRepState",
    "ProbeRepStatus",
    "ProbeRunState",
    "default_local_probes_state_path",
    "ProbeSideOutcome",
    "ProbeSideSummary",
    "run_probes_side",
]

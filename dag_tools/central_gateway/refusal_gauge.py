"""The REFUSAL-REASON GAUGE — how many requests each refusal reason accounts for, by name.

**This module measures. It never refuses.** Nothing here can change the outcome of a request.
Same posture, and the same blanket-except discipline at the call site, as `subject_gauge` and
`principal_gauge`.

## Why a count, when the reason is already in the log

`IDENTITY_WOULD` has carried `reason=<name>` since the observe posture shipped, so the names are
on the wire. Counting them meant scraping INFO lines — and a deployment that drops INFO counts
nothing while looking exactly like a deployment with nothing to count. That is the
zero-because-silent failure `observed` exists to separate everywhere else in this package, so
the reasons get the same treatment: a number per name on `/health`, with `observed` beside them.

## TWO populations, never one number

Under `observe` a request can be refused in two quite different senses, and a single count would
describe neither:

``would_refuse`` — what `enforce` WOULD have refused. The request was **served anyway**. This is
                   the migration-readiness number: how much of today's traffic breaks the day
                   the posture flips, broken out by what breaks it.
``did_refuse``   — what this gateway ACTUALLY refused. The caller got a 4xx. Under `observe` the
                   only reason that can appear here is `no_principal`; under `enforce` the two
                   populations are one decision, so a refusal is counted in both.

Collapsing them would tell an operator that N requests are failing when N-k of them are being
served, or the reverse — the same reason the read audit decomposes its gap instead of summing it.

## Why the vocabulary is installed rather than written down here

`identity` owns the reason names, and this module may not import it (`identity` imports this).
So `identity` hands its own vocabulary over at import: a reason it adds is counted the day it is
added, rather than on the day it is first refused by. Every installed name is always present, at
0, in both maps — a key that appears only once it is non-zero gets read with `.get()` and is then
indistinguishable from one that was merely misspelled.

``undeclared`` is the one key not from that vocabulary. A reason nothing installed is counted
there and warned about once, so the key sets stay stable and the surprise stays loud. It should
be unreachable: the vocabulary is derived from the same mapping that gives every reason its
status code.
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Iterable, Optional

logger = logging.getLogger(__name__)

#: Stable keys. Grep targets, not prose.
WOULD = "would_refuse"
DID = "did_refuse"
UNDECLARED = "undeclared"
OBSERVED = "observed"

#: Process-wide counts. Locked because the gateway serves requests concurrently and a lost
#: increment under-reports the one direction this instrument must not be wrong in.
_LOCK = threading.Lock()
_VOCABULARY: tuple = ()
_COUNTS: Dict[str, Any] = {OBSERVED: 0, WOULD: {UNDECLARED: 0}, DID: {UNDECLARED: 0}}
_WARNED: set = set()


def install_vocabulary(names: Iterable[str]) -> None:
    """Declare the reason names to count, each starting at 0 in both maps.

    Called by `identity` at import with its own reason vocabulary. Idempotent, and it never
    clears a count: calling it twice adds whatever is new and leaves the rest where it is.
    """
    global _VOCABULARY
    with _LOCK:
        merged = list(_VOCABULARY) + [n for n in names if n not in _VOCABULARY]
        _VOCABULARY = tuple(merged)
        for bucket in (WOULD, DID):
            for name in _VOCABULARY:
                _COUNTS[bucket].setdefault(name, 0)


def vocabulary() -> tuple:
    """The installed reason names, in installation order. For tests and for `announce`."""
    with _LOCK:
        return _VOCABULARY


def counts() -> Dict[str, Any]:
    """A snapshot: `observed`, plus one count per reason name in each of the two maps.

    Served on `/health`. `observed` is the discriminator and the reason this is not two numbers:
    every reason at 0 is a clean population when `observed` is large and an uninstalled gauge
    when `observed` is 0, and those are the two states an instrument exists to separate.

    Counts are per PROCESS and since start-up: a replica that restarts starts from 0, and a
    deployment reads them per pod. Nothing here persists or aggregates, deliberately — an
    instrument that needs a backing store is one more thing that can fail on a read path.
    """
    with _LOCK:
        return {
            OBSERVED: _COUNTS[OBSERVED],
            WOULD: dict(_COUNTS[WOULD]),
            DID: dict(_COUNTS[DID]),
        }


def reset_counts() -> None:
    """Zero every count, keeping the vocabulary. For tests: counts otherwise leak between them."""
    with _LOCK:
        _COUNTS[OBSERVED] = 0
        for bucket in (WOULD, DID):
            for name in _COUNTS[bucket]:
                _COUNTS[bucket][name] = 0
        _WARNED.clear()


def announce() -> str:
    """Say at startup that this gauge is wired, so silence can be read.

    Without it, an operator finding every reason at 0 cannot tell a clean population from an
    instrument that was never installed — the same zero-because-silent failure `observed` guards
    one level down. The line travels this module's own logger, so it is also the visibility
    check: if it is missing, every reading below it would have been missing too.
    """
    line = (
        "refusal-reason gauge: counting refusals by reason name — %d names installed, in two "
        "populations (%s: what enforce would refuse, with the request served anyway; %s: what "
        "this gateway actually refused). This instrument refuses nothing."
    ) % (len(vocabulary()), WOULD, DID)
    logger.info(line)
    return line


def _bump(bucket: str, reason: str) -> None:
    """Increment one name in one map. The caller holds the lock."""
    if reason in _COUNTS[bucket]:
        _COUNTS[bucket][reason] += 1
        return
    _COUNTS[bucket][UNDECLARED] += 1
    if reason not in _WARNED:
        _WARNED.add(reason)
        logger.warning(
            "REFUSAL REASON UNDECLARED: this reason is not in the installed vocabulary, so it "
            "is counted as %s rather than by name. Install it with the others: %s",
            UNDECLARED, reason,
        )


def observe(
    *,
    urn: str,
    would_refused: Optional[str],
    did_refused: Optional[str],
    enforcing: bool,
) -> Dict[str, Any]:
    """Count ONE resolved request and return the reading, for tests.

    `would_refused` is the reason `enforce` would have refused with, or None. `did_refused` is
    the reason this request was actually refused with, or None. Under `enforce` they are one
    decision and arrive equal; under `observe` a `would_refused` request was still served.

    Called for its counting. **Nothing in the caller may branch on this**: the moment a request
    outcome depends on it, it stops being a gauge and becomes an unreviewed enforcement path.
    """
    with _LOCK:
        _COUNTS[OBSERVED] += 1
        if would_refused:
            _bump(WOULD, would_refused)
        if did_refused:
            _bump(DID, did_refused)

    logger.info(
        "refusal-reason: would=%s did=%s posture=%s urn=%s",
        would_refused or "-", did_refused or "-", "enforce" if enforcing else "observe", urn,
    )
    return {WOULD: would_refused, DID: did_refused}

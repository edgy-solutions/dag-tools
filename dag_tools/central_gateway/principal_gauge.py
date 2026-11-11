"""The PRINCIPAL-RECORD GAUGE — whether the record of a read names who it was for.

**This module measures. It never refuses.** Nothing here can change the outcome of a request:
no 401, no 403, no header read, no subject altered. It is behaviourally inert by construction,
which is the only reason it is safe to ship onto a live read path. Same posture, and the same
blanket-except discipline at the call site, as `subject_gauge`.

## The defect it sized, and why it is still here

**The divergence below is CLOSED.** `identity.resolve_principal` is now the one resolution of
who a request reads as, and the gate and the record both read it, so `unnamed` and `divergent`
are structural impossibilities rather than the daily occurrence described here.

The gauge stays, and its counts go on `/health`, because that is the only evidence the closure
is still true. A check deleted the day it passes measures nothing afterwards: the next edit to
the route is free to reintroduce a second resolution, and nothing would say so. `observed` is
what makes the zeros mean something -- every other count being 0 says nothing at all until
`observed` is not 0.

What follows is the shape it was built to size, kept because it is also the shape it watches
for.

## The defect it exists to size

A read is authorized for a subject, and then it is recorded — in the payload the gateway sends
the broker, and in the `TICKET_ISSUED` line. Those two agree with each other by construction:
they read ONE expression, exactly so an audit trail cannot start describing someone other than
the decision did.

What nothing checked is whether that expression agrees with **the subject the decision was
actually made for**, and under the OBSERVE posture it did not always:

* the authorization subject is the header's, falling back to the token's own claim;
* the recorded subject is the header's, with **no fallback**.

So a request arriving with a token that carries a subject and no `X-Originator-Email` /
`on_behalf_of` header is authorized for the token's principal and recorded as nobody: the broker
is handed ``"subject": null`` and the issue line prints ``subject=-``. Under `enforce` both read
the verified decision and the question did not arise.

That shape is not an edge case. It is `subject_gauge`'s `token-claim` bucket — the one its own
documentation calls *the healthy shape* — so the healthiest way for identity to arrive is also
the one way the record of the read names nobody.

## Why it measured first instead of fixing

The fix was one expression, and it was not obviously the right one to apply unobserved. The
recorded subject is not decoration: a `mint-role` backend encodes it into a role's GRANTs and
ROW POLICY, so starting to send a subject where `null` went before changes what a credential
grants, for callers nobody has enumerated. And the subject available to fall back on was read
from an **unverified** token, which is a defensible thing to log and a different thing to
authorize a credential against.

Counting the population first is what answered that. The answer was the third option: an
unverified claim names nobody, so the request is refused rather than served under either
subject. Nothing now sends a subject where `null` went before *except* where the token was
verified, which is the one case where the name was never in doubt.

## Reading the buckets

``named``      — the record names the principal the decision was made for. The whole population
                 should be this.
``unnamed``    — the decision named a principal; **the record names nobody.** The finding.
``divergent``  — both name a principal and they are not the same one. Should be unreachable;
                 if it is ever reported, it is worse than `unnamed`, because the record is not
                 merely silent but wrong.
``undecided``  — the decision itself resolved no subject. Sited where it is — after the gate
                 allowed — this should not occur, because a gate that resolves no subject
                 fail-closed denies. Reported rather than assumed away.
``unreadable`` — no subject-source reading was available, so this request could not be
                 classified. **Never counted as clean.** Zero-because-silent and
                 zero-because-clean are the two states an instrument exists to separate, and
                 this one is downstream of another instrument that can itself fail.
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)

#: Stable log tokens — grep targets, not prose.
REC_NAMED = "named"
REC_UNNAMED = "unnamed"
REC_DIVERGENT = "divergent"
REC_UNDECIDED = "undecided"
REC_UNREADABLE = "unreadable"

_BUCKETS = (REC_NAMED, REC_UNNAMED, REC_DIVERGENT, REC_UNDECIDED, REC_UNREADABLE)

#: Process-wide counts, one per bucket plus `observed`. Locked because the gateway serves
#: requests concurrently and a lost increment here under-reports the one direction this
#: instrument must not be wrong in.
#:
#: EVERY key is always present, at 0. A key that appears only once it is non-zero gets read
#: with `.get()` and is then indistinguishable from one that was merely misspelled.
_LOCK = threading.Lock()
_COUNTS: Dict[str, int] = {"observed": 0, **{bucket: 0 for bucket in _BUCKETS}}


def counts() -> Dict[str, int]:
    """A snapshot of every bucket, plus `observed`. Served on `/health`.

    `observed` is the discriminator and the reason this is a dict rather than one number:
    `unnamed=0` is a closed defect when `observed` is large and an uninstalled gauge when
    `observed` is 0, and those are the two states an instrument exists to separate. The same
    reason `telemetry_misses` is read as a number after a roll rather than as the absence of a
    warning in a log.

    Counts are per PROCESS and since start-up: a replica that restarts starts from 0, and a
    deployment reads them per pod. Nothing here persists or aggregates, deliberately -- an
    instrument that needs a backing store is one more thing that can fail on a read path.
    """
    with _LOCK:
        return dict(_COUNTS)


def reset_counts() -> None:
    """Zero the counters. For tests: a process-wide count otherwise leaks between them."""
    with _LOCK:
        for key in _COUNTS:
            _COUNTS[key] = 0


def announce() -> str:
    """Say at startup that this gauge is wired, so silence can be read.

    Without it, an operator finding no ``principal-record:`` lines cannot tell a clean
    population from an instrument that was never installed — which is the same
    zero-because-silent failure the ``unreadable`` bucket exists to prevent one level down.
    The announcement travels this module's own logger, so it is also the visibility check:
    if this line is missing, every reading below it would have been missing too.
    """
    line = (
        "principal-record gauge: OBSERVE — measuring whether each issued ticket records the "
        "principal it was authorized for. This instrument refuses nothing."
    )
    logger.info(line)
    return line


def classify(recorded: Optional[str], decided: Optional[str]) -> str:
    """Which bucket one request falls in, given both subjects."""
    rec = (recorded or "").strip() or None
    dec = (decided or "").strip() or None

    if dec is None:
        return REC_UNDECIDED
    if rec is None:
        return REC_UNNAMED
    return REC_NAMED if rec == dec else REC_DIVERGENT


def observe(
    *,
    urn: str,
    recorded: Optional[str],
    source_reading: Optional[Mapping[str, Any]],
    enforcing: bool,
) -> Dict[str, Any]:
    """Measure ONE issued ticket and emit ONE line. Returns the reading, for tests.

    ``source_reading`` is `subject_gauge.observe`'s return value, or ``None`` when that gauge
    failed — which is a reading of its own and must not read as clean.

    Called for its logging. **Nothing in the caller may branch on this**: the moment a request
    outcome depends on it, it stops being a gauge and becomes an unreviewed enforcement path.
    """
    if source_reading is None:
        decided: Optional[str] = None
        bucket = REC_UNREADABLE
    else:
        decided = source_reading.get("effective_subject")
        bucket = classify(recorded, decided)

    with _LOCK:
        _COUNTS["observed"] += 1
        _COUNTS[bucket] = _COUNTS.get(bucket, 0) + 1

    logger.info(
        "principal-record: record=%s recorded_present=%s decided_present=%s posture=%s urn=%s",
        bucket,
        bool((recorded or "").strip()),
        bool((decided or "").strip()),
        "enforce" if enforcing else "observe",
        urn,
    )

    # The one line an operator greps for. Both warning cases are about the record of a read
    # that was ALLOWED and SERVED, so neither is visible anywhere else: the request looks
    # entirely healthy from every other angle.
    if bucket == REC_UNNAMED:
        logger.warning(
            "PRINCIPAL UNRECORDED: this read was authorized for a subject and recorded "
            "against nobody — the broker payload carries a null subject and TICKET_ISSUED "
            "names '-'. The gate and the record read ONE resolution, so this should be "
            "unreachable: it means a second resolution has been reintroduced. urn=%s", urn,
        )
    elif bucket == REC_DIVERGENT:
        logger.warning(
            "PRINCIPAL MISRECORDED: the record names a different principal than the "
            "authorization decision was made for. urn=%s", urn,
        )
    elif bucket == REC_UNREADABLE:
        logger.warning(
            "PRINCIPAL RECORD UNREADABLE: no subject-source reading for this request, so it "
            "is uncounted rather than clean. urn=%s", urn,
        )

    return {"record": bucket, "recorded": recorded, "decided": decided}

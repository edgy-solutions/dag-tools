"""Refuse to hand a reader a credential this broker did not mint.

THE EXPOSURE. invincible-agent ADR-0044's rollout is per backend, so a ticket for a backend
with no minter was served as-is -- and ``_sanitize_ticket`` puts the
producer's echoed ``credentials`` back on exactly those tickets, because for
an unminted backend that credential is the only way to read the data. So the
reader received the producer's credential: a write credential, unscoped, not
expiring with the access window. It was counted (``UNPROTECTED_SOURCE_TYPES``)
and labelled (``mode: "producer-credential-unprotected"``), which is how it
was found, but counting an exposure is not closing it.

WHY A POSTURE RATHER THAN A REFUSAL. Refusing today breaks every read of a
backend with no minter, which is a working deployment's data access, so the
default is ``observe``: today's behaviour exactly, plus a number per backend
and a line per request. A deployment that has finished its rollout -- or that
would rather lose the read than leak the credential -- sets
``BROKER_UNMINTED_POSTURE=enforce``. Same two-posture shape, same parser and
same fail-closed-on-garbage reading as ``central_gateway.internal_door``; the
grammar is not written down a second time.

WHAT THE SEAM MAY NOT DECIDE FROM. Two plausible discriminators are both
wrong, and each would read as protection:

* ``source_type``. It says which backend the ticket names, not where its
  credential came from. The legacy coordinate path in ``main.py`` returns a
  ``postgres`` ticket carrying a hardcoded placeholder token, and ``postgres``
  is in ``_MINTERS`` -- so membership would call that ticket minted.
* ``mode``. ``_sanitize_ticket`` preserves a producer-supplied one
  (``clean.get("mode") or ...``), so a producer could advertise
  ``mode: "mint-sts"`` on an unminted backend and keep its credentials.

So each resolve path SAYS whether the broker minted what it is returning
(``main.Resolved``), and the decision is taken once, at the one seam every
path returns through (AGENTS.md section 4o). A path added later that returns a
bare dict fails loudly rather than being read as minted.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from dag_tools.central_gateway.identity import (
    POSTURE_ENFORCE,
    POSTURE_OBSERVE,
    parse_posture,
)

logger = logging.getLogger(__name__)

POSTURE_ENV = "BROKER_UNMINTED_POSTURE"

REASON = "unminted_credential"
"""The one refusal reason. A token rather than a sentence: it is a dict key in
the counters below and the whole of the refusal body."""

ABSENT_SOURCE_TYPE = "(absent)"
"""Counter key for a ticket advertising no ``source_type``.

THE DANGEROUS SUB-CASE, and it needs a visible key. ``_MINTERS.get("")``
misses, so a ticket with an absent or blank ``source_type`` is unminted --
which is right -- but counted under ``""`` it reads on ``/health`` as a
rendering artefact rather than as a backend nobody can name. Deliberately not
``"unknown"``: ``AS_OF_ABSENT`` already uses that word one section down in the
same document, for a different question."""

WOULD_REFUSE: Dict[str, int] = {}
"""source_type -> reads that WOULD be refused if the posture were enforce.

Under ``observe`` these reads are SERVED anyway, which is the migration
signal: what breaks when the posture flips. Never summed with ``REFUSED``
(AGENTS.md section 4o-1) -- one number would report refusals for reads that
were served. Under ``enforce`` the two are one decision and a refusal counts
in both, which is the one state in which their agreeing is correct."""

REFUSED: Dict[str, int] = {}
"""source_type -> reads actually refused. Empty under ``observe``, always."""


def posture() -> str:
    """``observe`` by default; anything unrecognised reads as ``enforce``."""
    return parse_posture(POSTURE_ENV)


def source_type_key(ticket: Dict[str, Any]) -> str:
    """Counter key for a ticket -- never the empty string."""
    return str(ticket.get("source_type") or "") or ABSENT_SOURCE_TYPE


def _bump(counter: Dict[str, int], key: str) -> None:
    counter[key] = counter.get(key, 0) + 1


def decide(ticket: Dict[str, Any], minted: bool) -> Optional[str]:
    """Return a refusal reason for this ticket, or None to serve it.

    A ticket the broker minted is served. So is an unminted ticket carrying no
    credentials: nothing is exposed, and refusing it would break a coordinates-
    only route for no gain. What is refused -- under ``enforce`` -- is an
    unminted ticket carrying a credential, which is a credential this broker
    did not create and cannot scope, bound for a reader.

    The count and the line sit in SEPARATE ``try`` blocks, and neither can
    fail the read: an instrument may never break the thing it measures, and
    one block would make whichever ran first the only one that survives a
    defect in the other.
    """
    if minted:
        return None
    if not ticket.get("credentials"):
        return None

    mode = posture()
    key = source_type_key(ticket)
    enforcing = mode == POSTURE_ENFORCE

    try:
        _bump(WOULD_REFUSE, key)
        if enforcing:
            _bump(REFUSED, key)
    except Exception:  # noqa: BLE001
        logger.exception("%s: counting failed; the read is unaffected", POSTURE_ENV)

    try:
        logger.warning(
            "invincible-agent ADR-0044: %s source_type=%s uri=%s reason=%s. The broker did not "
            "mint this credential -- it is the producer's, unscoped and not "
            "expiring with the access window. The fix is a minter for this "
            "backend in domain_broker.main._MINTERS; until then %s=%s decides "
            "whether the read is served. Counted in "
            "/health.adr0044.unminted_credentials.",
            "refused" if enforcing else "would_refuse",
            key,
            ticket.get("physical_uri"),
            REASON,
            POSTURE_ENV,
            mode,
        )
    except Exception:  # noqa: BLE001
        pass

    return REASON if enforcing else None


def health() -> Dict[str, Any]:
    """The posture and both numbers, for ``/health.adr0044``.

    Answers rather than raising: a readiness or liveness probe that 500s
    because a counter misbehaved takes the pod out over an instrument."""
    try:
        return {
            "posture": posture(),
            "would_refuse": dict(WOULD_REFUSE),
            "refused": dict(REFUSED),
        }
    except Exception:  # noqa: BLE001
        logger.exception("%s: reporting failed", POSTURE_ENV)
        return {"posture": None, "would_refuse": None, "refused": None}


__all__ = [
    "ABSENT_SOURCE_TYPE",
    "POSTURE_ENFORCE",
    "POSTURE_ENV",
    "POSTURE_OBSERVE",
    "REASON",
    "REFUSED",
    "WOULD_REFUSE",
    "decide",
    "health",
    "posture",
    "source_type_key",
]

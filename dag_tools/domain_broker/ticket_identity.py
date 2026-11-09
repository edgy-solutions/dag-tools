"""Ticket identity: which ticket this is, when it was issued, and over what.

A routing ticket used to be a pure coordinate-plus-credential: everything in it
described WHERE to read and nothing identified the act of issuing it. Two
consequences, both found from the operator's side rather than from review.

A read could not be referred to. The gateway logs the authorization decision
and the broker logs the minting, but nothing tied the two to the frame a
consumer ended up holding -- so "which read was this?" had no answer, and that
answer is where an audit, a complaint and a bill all start. ``ticket_id`` is
the answer, and ``issued_at`` is when the decision behind it was made.

And a frame could not be pinned to a version. A consumer reads a dataset that
is being rewritten underneath them; two agents answering the same question
minutes apart legitimately disagree, and neither can say so. ``as_of`` is the
dataset's own snapshot or version -- a watermark, a load id, a table version --
so a frame can be cited rather than merely dated.

THE ONE RULE THIS MODULE EXISTS TO ENFORCE: ``as_of`` is the data's version,
``issued_at`` is the time we looked. They are never the same field, and an
absent ``as_of`` is NEVER filled in with the issue time. That substitution is
tempting precisely because it makes every ticket look complete, and it would
make the field worthless: a reader cannot tell a version from a clock reading,
so it would have to assume the worst of every ticket, including the ones
carrying a real version. An absent as-of is reported as absent, with a reason,
and counted.

Values are carried AS THE SOURCE WROTE THEM, the rule
``dag_tools.warehouse_source.observe.read_as_of`` already follows for the same
reason: reformatting makes two equal watermarks look different.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

# Keys a producer may use to advertise its own version in the coordinates it
# publishes at load time, checked in this order. A producer that advertises
# none is not a problem -- it has no as-of to offer, which is reported rather
# than guessed at.
ADVERTISED_AS_OF_KEYS: Tuple[str, ...] = ("as_of", "snapshot_id", "version")

# Where an as-of came from. The distinction is load-bearing: coordinates are
# read ONCE at broker startup and live in ``LOCAL_ASSETS`` for the process
# lifetime, so a value taken from them describes the dataset as it was when
# this broker booted, not as it is now. Presenting that as the current snapshot
# would be the same load-time trap the credential path already refuses (a
# credential minted at load is one credential shared by every caller) -- so the
# ticket says which it is and lets the consumer decide whether it is enough.
SOURCE_READ_AT_AUTHORIZE = "read_at_authorize"
SOURCE_ADVERTISED_AT_LOAD = "advertised_at_load"

# Why there is no as-of. Tokens, not sentences: they are counted per source
# type, and a sentence does not stay a stable dictionary key for long.
ABSENT_NO_READER = "no_reader_for_source_type"
ABSENT_READER_DECLINED = "reader_declined"
ABSENT_READER_FAILED = "reader_failed"
ABSENT_UNUSABLE_VALUE = "reader_returned_unusable_value"


@dataclass(frozen=True)
class TicketIdentity:
    """The identity stamped onto one issued ticket.

    ``as_of`` and ``as_of_source`` stand or fall together: a version with no
    stated provenance cannot be reasoned about, so neither is ever set alone.
    """

    ticket_id: str
    issued_at: str
    as_of: Optional[str] = None
    as_of_source: Optional[str] = None
    as_of_unavailable: Optional[str] = None

    def fields(self) -> Dict[str, Any]:
        """The ticket keys this identity contributes.

        ``as_of`` is always present, as ``None`` when unknown, so a consumer
        reads one shape from every backend. A KEY THAT APPEARS ONLY SOMETIMES
        gets read with ``.get()`` and is then indistinguishable from one that
        was merely misspelled. The reason appears only when there is one.
        """
        out: Dict[str, Any] = {
            "ticket_id": self.ticket_id,
            "issued_at": self.issued_at,
            "as_of": self.as_of,
            "as_of_source": self.as_of_source,
        }
        if self.as_of_unavailable is not None:
            out["as_of_unavailable"] = self.as_of_unavailable
        return out


# source_type -> a callable taking the advertised ticket and returning that
# dataset's CURRENT version, or None when it has none yet. Absent from this
# table means the backend has no version concept wired up, and the ticket says
# so -- the same posture as _MINTERS, which reports a backend with no minter
# rather than quietly passing it over.
#
# A reader runs INSIDE an authorize call, so it must be cheap, and it must not
# raise to any purpose: a failure degrades to an absent as-of, it never fails a
# ticket. An as-of is metadata about the read; a credential is the read. Only
# one of the two is worth refusing a consumer over.
AS_OF_READERS: Dict[str, Callable[[Mapping[str, Any]], Optional[str]]] = {}


def register_as_of_reader(
    source_type: str,
    reader: Callable[[Mapping[str, Any]], Optional[str]],
) -> None:
    """Teach the broker to read one backend's version at authorize time."""
    AS_OF_READERS[source_type] = reader


def _usable(value: Any) -> Optional[str]:
    """The value as text, or None if it cannot honestly be carried as one.

    Integers convert because a table version IS an integer and ``str`` of one
    is faithful. Anything structured is refused rather than stringified: a
    dict's repr in a ticket field is not a version, and two consumers comparing
    them would be comparing Python syntax.
    """
    if value is None:
        return None
    if isinstance(value, bool):  # bool is an int; a flag is not a version
        return None
    if isinstance(value, str):
        return value or None
    if isinstance(value, int):
        return str(value)
    return None


def _advertised(ticket: Mapping[str, Any]) -> Optional[str]:
    """The version the producer published in its coordinates, if any."""
    for key in ADVERTISED_AS_OF_KEYS:
        value = _usable(ticket.get(key))
        if value is not None:
            return value
    return None


def resolve_as_of(
    source_type: str,
    ticket: Mapping[str, Any],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """``(as_of, as_of_source, as_of_unavailable)`` for one ticket.

    A version read at authorize time beats one advertised at load, because the
    first describes the dataset the consumer is about to read and the second
    describes it as it was when this process started.
    """
    reader = AS_OF_READERS.get(source_type or "")
    if reader is None:
        advertised = _advertised(ticket)
        if advertised is not None:
            return advertised, SOURCE_ADVERTISED_AT_LOAD, None
        return None, None, ABSENT_NO_READER

    try:
        raw = reader(ticket)
    except Exception as exc:  # noqa: BLE001 - a reading cannot fail the read
        logger.warning(
            "as-of reader for source_type %r failed (%s: %s) - issuing the "
            "ticket with no as-of rather than refusing it.",
            source_type, type(exc).__name__, exc,
        )
        return None, None, ABSENT_READER_FAILED

    value = _usable(raw)
    if value is not None:
        return value, SOURCE_READ_AT_AUTHORIZE, None

    if raw is not None:
        logger.warning(
            "as-of reader for source_type %r returned %s, which cannot be "
            "carried as a version; issuing the ticket with no as-of.",
            source_type, type(raw).__name__,
        )
        return None, None, ABSENT_UNUSABLE_VALUE

    # The reader ran and said this dataset has no version yet. That is an
    # answer, and a different one from having no reader at all -- but a stale
    # advertised version is still better than nothing, provided it is labelled.
    advertised = _advertised(ticket)
    if advertised is not None:
        return advertised, SOURCE_ADVERTISED_AT_LOAD, None
    return None, None, ABSENT_READER_DECLINED


def mint(
    source_type: str,
    ticket: Mapping[str, Any],
    *,
    now: Optional[datetime] = None,
) -> TicketIdentity:
    """Mint an identity for one ticket, at the moment it is issued.

    PER REQUEST, never cached. ``LOCAL_ASSETS`` holds a producer's advertised
    coordinates for the broker's whole lifetime, so an identity minted there
    would hand every caller of every asset the same ticket id and an issue time
    from whenever the pod started. A log that cannot distinguish two reads is
    worse than no log, because it reads as though it can.
    """
    moment = now or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        # A naive timestamp in an audit field is a timestamp in an unknown
        # zone: readers assume UTC and are wrong by hours twice a year.
        moment = moment.replace(tzinfo=timezone.utc)
    as_of, as_of_source, absent = resolve_as_of(source_type, ticket)
    return TicketIdentity(
        ticket_id=str(uuid.uuid4()),
        issued_at=moment.astimezone(timezone.utc).isoformat(),
        as_of=as_of,
        as_of_source=as_of_source,
        as_of_unavailable=absent,
    )


def stamp(
    ticket: Mapping[str, Any],
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """A copy of ``ticket`` carrying its identity.

    Applied at the ONE seam every resolve path returns through, so a path that
    mints no credential -- a backend with no minter, a legacy coordinate route
    -- is identified too. A read the broker declined to protect is the one most
    worth being able to refer to afterwards.

    The producer's own keys are left alone: an advertised version stays where
    it was and is additionally reported as this ticket's as-of, so a consumer
    reading the old key keeps working.
    """
    identity = mint(str(ticket.get("source_type") or ""), ticket, now=now)
    return {**dict(ticket), **identity.fields()}

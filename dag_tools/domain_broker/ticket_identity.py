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

from dag_tools.domain_broker.as_of_readers import SHIPPED_AS_OF_READERS

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


# invincible-agent #529 -- the readers THIS BUILD SHIPS, which is a different
# fact from the readers this process registered. One table, read by three
# things: the chart's template constant at render time, the startup check in
# ``main`` at start, and ``/health`` at runtime. If any of the three grows its
# own list they disagree silently and each one individually looks correct.
#
# It is NOT written down here: it is ``as_of_readers.SHIPPED_AS_OF_READERS``,
# because a second copy of a list read by three things is the disagreement this
# comment exists to warn about. That module imports nothing from here and
# nothing heavy at all -- each driver import is inside its own reader -- so this
# import cannot make the broker unimportable on an install that lacks one.
#
# It shipped EMPTY until the two measured readers landed, and the reason it did
# is worth keeping in view: a reader for a backend is real IO against a real
# store, and one whose only evidence is that it parses is a half-measure. What
# is here has been exercised against a real store of its kind; what has not is
# still absent, which is why a selection naming a format this build does not
# read is still refused rather than accepted and ignored.
AVAILABLE_AS_OF_READERS: Dict[str, Callable[[Mapping[str, Any]], Optional[str]]] = dict(
    SHIPPED_AS_OF_READERS
)

# The deployment names the subset it wants. Read ONCE, at module scope, never
# per request: a config error consulted inside an authorize call is a reading
# that can fail a read, which is the one thing this module does not allow.
AS_OF_READERS_ENV = "DAG_TOOLS_AS_OF_READERS"


class UnknownAsOfReader(ValueError):
    """A deployment asked for an as-of reader this build does not ship.

    A ``ValueError`` subclass so anything already catching ``ValueError``
    catches it, the same courtesy ``MissingCredentials`` extends.
    """


def available_as_of_readers() -> Tuple[str, ...]:
    """The source types this BUILD ships a reader for. Static, known at import.

    This is the set a render-time check compares a deployment's list against,
    and it is why that check can run with no pod: it is a property of the
    pinned package, not of a running process.
    """
    return tuple(sorted(AVAILABLE_AS_OF_READERS))


def registered_as_of_readers() -> Tuple[str, ...]:
    """The source types THIS PROCESS has a reader registered for.

    READ AT CALL TIME, never snapshotted at import: ``register_as_of_reader``
    mutates the registry, so a module that registers late would be invisible to
    a snapshot taken early. A render check cannot answer this question -- no pod
    exists yet -- and this is the only thing that knows what a pod actually did.
    """
    return tuple(sorted(AS_OF_READERS))


def select_as_of_readers(value: Optional[str]) -> Tuple[str, ...]:
    """The source types named by one ``DAG_TOOLS_AS_OF_READERS`` value.

    Pure: it validates and returns names, and registers nothing. An unset or
    empty value selects nothing, which is today's behaviour exactly -- the
    setting ships inert.

    A name this build does not ship RAISES, because it is not a reading that
    might fail; it is a typo in configuration, and this repo's standing answer
    to a typo is that it fails at load and not at 3am. EVERY unknown name is
    named at once, and so is the available set, so the fix is in the message
    and nobody has to go and find the table.

    An empty entry -- a trailing comma, a doubled one -- names no source type,
    so it is dropped rather than refused.
    """
    if value is None:
        return ()
    names = [part.strip() for part in value.split(",")]
    wanted = sorted({name for name in names if name})
    unknown = [name for name in wanted if name not in AVAILABLE_AS_OF_READERS]
    if unknown:
        available = available_as_of_readers()
        offer = ", ".join(available) if available else (
            "none -- this build ships no as-of reader yet, so any name is "
            "refused until one lands and the deployment repins"
        )
        raise UnknownAsOfReader(
            "%s names %s, which this build of dag-tools does not ship a reader "
            "for. Available: %s."
            % (AS_OF_READERS_ENV, ", ".join(unknown), offer)
        )
    return tuple(wanted)


def register_selected_as_of_readers(value: Optional[str]) -> Tuple[str, ...]:
    """Validate one env value and register what it names. Returns the names.

    Refuses before registering anything, so a value with one bad name among
    good ones leaves the registry exactly as it was: a half-applied selection
    is a deployment that got some of what it asked for and was told it got
    none of it.
    """
    selected = select_as_of_readers(value)
    for name in selected:
        register_as_of_reader(name, AVAILABLE_AS_OF_READERS[name])
    return selected


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

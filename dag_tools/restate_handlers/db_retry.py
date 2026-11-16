"""Retry bounds and terminal classification for the handlers' database steps.

``ctx.run`` retries whatever raises. For an HTTP step that is merely
wasteful; for a step that opens a database connection it is destructive,
because a failed login is not a free operation. Every attempt spends one
of the account's failed-login attempts, and the server locks the account
when that budget runs out -- Oracle's ``FAILED_LOGIN_ATTEMPTS`` defaults
to 10, and sites that have tightened it are common. So an unbounded retry
of a wrong password ends as a locked account: the handler converts a
one-line configuration mistake into an outage, and the lockout then takes
down every other consumer of that account, including the ones that had
the right password all along.

The invariant worth naming is narrower than "an authentication error is
permanent". It is that **retrying this particular error consumes
something that does not come back**, which is why it is not enough to
fail eventually -- it has to fail on the first attempt.

Two mechanisms, and both are needed.

**Classification** makes the recognised case terminal immediately. A code
this module knows to be damaging is converted to
``restate.TerminalError``, which Restate does not retry. A wrong password
causes one login attempt, not a loop.

**A retry bound** is the backstop classification cannot be. A code list
is a list of what has been seen; an unrecognised authentication error
would still loop forever without a bound on the number of attempts. The
bound is what makes the damage finite even for an error nothing here
recognises, and the classification is what makes the recognised ones
legible at once instead of after the bound.

Nothing here names a database, a schema, a table or a column. The error
codes are a *driver's* vocabulary, in exactly the sense that the HTTP
statuses in :mod:`dag_tools.restate_handlers.api_call_plan` are a
protocol's -- they are not names from below.

The classifier reads ``str(exc)``, and so imports no driver. ``oracledb``
and ``pyodbc`` both put the vendor's code in the text of the exception,
and SQLAlchemy repeats it because it wraps the DBAPI error and quotes
it -- so one classifier serves all three call sites and none of those
libraries has to be installed for this module to import.

One failure here is not a database failure at all. A step handed a
payload it cannot act on is the *same* question -- retrying it cannot
succeed, because every attempt is handed the same payload -- and it
belongs in the one place that answers it, rather than being decided
again in each handler. :class:`MalformedPayload` is how a handler says
so. Validation that stays *outside* the step is worse than unbounded:
a raise there fails the whole invocation, which Restate retries with no
bound at all and no journal entry to show what was tried.
"""
import asyncio
import functools
import os
import re
from datetime import timedelta
from typing import Any, Callable, Dict, Optional, Tuple

import restate


class MissingCredentials(ValueError):
    """Connection credentials are absent from the worker's environment.

    A ``ValueError`` subclass, so a caller that already catches
    ``ValueError`` keeps working.

    This is terminal on the first failure for the same reason the
    ``_connect`` docstring had to be corrected: a container's environment
    is snapshotted when the process starts, so a variable that is absent
    now is absent for the life of the pod. Retrying cannot find it, and
    the retry that cannot succeed is the one that sits in the journal
    looking like a transient problem.
    """


class MalformedPayload(ValueError):
    """The invocation's payload cannot be acted on.

    A ``ValueError`` subclass for the same reason as
    :class:`MissingCredentials` (AGENTS section 6: a caller that already
    catches ``ValueError`` keeps working), and terminal for a reason of
    the same shape: a retry is handed the same payload, so nothing
    changes between attempts except the clock.

    Raise it *inside* the step. Raised outside one it leaves the step's
    guard behind entirely -- the invocation fails, and an invocation
    that fails on a non-terminal error is retried for the life of the
    worker.

    The message travels into the ``TerminalError``, because it is ours
    rather than a driver's -- which makes it the handler's job to name
    the fields it refused and no value from any of them. A payload names
    a table and a column, and a ``TerminalError``'s message reaches the
    journal and the caller.
    """


#: Retrying these *spends* something. Each attempt consumes one of the
#: account's failed-login attempts, so the retry is itself what locks the
#: account out -- which is why they are terminal on the first failure
#: rather than merely bounded.
DAMAGING_ORACLE_CODES: Tuple[str, ...] = (
    "ORA-01017",  # invalid credential; logon denied
    "ORA-01005",  # null password given
    "ORA-28000",  # the account is locked -- the budget is already spent
    "ORA-28001",  # the password has expired
    "ORA-28008",  # invalid old password
    "ORA-28040",  # no matching authentication protocol
    "ORA-01045",  # the account lacks CREATE SESSION
)

#: The same class of failure as reported by SQL Server. Matched as a
#: standalone number, because these arrive inside a sentence rather than
#: behind a prefix; a false positive here fails an invocation loudly,
#: which is the safe direction to be wrong in.
DAMAGING_SQLSERVER_CODES: Tuple[int, ...] = (
    18456,  # login failed for user
    18486,  # the account is locked out
    18487,  # the password has expired
    18488,  # the password must be changed
)

#: Retrying these cannot damage anything, but it also cannot ever
#: succeed: nothing changes between attempts except the clock. A
#: separate and deliberately short tier, because the reasoning differs
#: and so does the cost of being wrong about it.
#:
#: Note what is NOT here. ``ORA-12541`` (no listener) and ``ORA-12170``
#: (connect timeout) look equally permanent and are not -- a listener
#: that is restarting answers on the next attempt, and that is precisely
#: the case ``ctx.run`` exists for.
PERMANENT_ORACLE_CODES: Tuple[str, ...] = (
    "ORA-12154",  # the connect identifier could not be resolved
)

_DAMAGING = "damaging"
_PERMANENT = "permanent"

_TIER_EXPLANATION = {
    _DAMAGING: (
        "retrying this spends another of the account's failed-login "
        "attempts, and the retry is what locks the account out, so this "
        "step failed on its first attempt rather than looping"
    ),
    _PERMANENT: (
        "retrying this cannot succeed without a configuration change, so "
        "this step failed on its first attempt rather than looping"
    ),
}

#: The reason reported for a credential the environment never carried.
#: A token rather than a sentence, the way ``ticket_identity`` reports
#: ``as_of_unavailable`` -- it is read by code as well as by a person.
CREDENTIALS_ABSENT = "credentials_absent_from_environment"

#: The reason reported for a payload the handler cannot act on. The
#: same kind of token, one step further out: the environment is read
#: once when the process starts, and the payload is fixed for the
#: invocation, so neither is something a retry can change.
PAYLOAD_MALFORMED = "payload_malformed"

#: Our own exceptions, whose messages DO travel into the
#: ``TerminalError``, with the sentence that says why a retry cannot
#: help. They travel because they are ours: each names a variable or a
#: field and carries no value from either, unlike a driver's text, which
#: quotes the connect identifier and sometimes the account name.
_OURS: Tuple[Tuple[type, str], ...] = (
    (
        MissingCredentials,
        "This cannot succeed on a retry: the container's environment is "
        "read once, when the process starts.",
    ),
    (
        MalformedPayload,
        "This cannot succeed on a retry: every attempt is handed the "
        "same payload.",
    ),
)

#: Attempts allowed for a step that connects. Three, which is under
#: Oracle's default ``FAILED_LOGIN_ATTEMPTS`` of 10 with room to spare,
#: and still spans Restate's backoff for long enough to ride out a
#: connection blip. It is the bound for the error the classifier does
#: NOT recognise; a recognised one never reaches a second attempt.
DEFAULT_MAX_ATTEMPTS = 3

MAX_ATTEMPTS_ENV = "DAGTOOLS_DB_MAX_ATTEMPTS"
MAX_RETRY_SECONDS_ENV = "DAGTOOLS_DB_MAX_RETRY_SECONDS"


def _standalone(code: int, text: str) -> bool:
    return re.search(r"(?<!\d)" + str(code) + r"(?!\d)", text) is not None


def classify(exc: BaseException) -> Optional[Tuple[str, str]]:
    """Why retrying ``exc`` is wrong, or ``None`` if retrying is fine.

    Returns ``(code, tier)``. The code is what the driver said; the tier
    is which of the two arguments above applies. ``None`` means this
    module has no opinion, and the retry bound is the only thing between
    the step and an unbounded loop.
    """
    if isinstance(exc, MissingCredentials):
        return (CREDENTIALS_ABSENT, _PERMANENT)
    if isinstance(exc, MalformedPayload):
        return (PAYLOAD_MALFORMED, _PERMANENT)

    text = str(exc).upper()
    for code in DAMAGING_ORACLE_CODES:
        if code in text:
            return (code, _DAMAGING)
    for number in DAMAGING_SQLSERVER_CODES:
        if _standalone(number, text):
            return (str(number), _DAMAGING)
    for code in PERMANENT_ORACLE_CODES:
        if code in text:
            return (code, _PERMANENT)
    return None


def _terminal_for(exc: BaseException) -> Optional[restate.TerminalError]:
    verdict = classify(exc)
    if verdict is None:
        return None
    code, tier = verdict
    for kind, sentence in _OURS:
        if isinstance(exc, kind):
            # These messages DO travel, because they are ours rather
            # than a driver's, and each is the single most useful thing
            # to put in front of whoever reads the failed invocation:
            # the fix is one line of deployment or one field of payload.
            return restate.TerminalError(f"{exc} {sentence}")
    # The message carries the code and a fixed sentence and NOTHING out
    # of ``str(exc)``. A driver's connection errors quote the connect
    # identifier, and some quote the account name, and a TerminalError's
    # message travels into the Restate journal and back to the caller.
    # The original is kept as the cause, so the worker's own log -- where
    # it already went -- still has every detail for whoever is debugging.
    return restate.TerminalError(f"{code}: {_TIER_EXPLANATION[tier]}")


def terminal_on_damage(action: Callable[[], Any]) -> Callable[[], Any]:
    """Wrap a ``ctx.run`` action so a damaging failure never gets retried.

    Applied at the call site rather than inside each handler's closure,
    so one reading of a handler shows both halves of the protection: the
    wrapper and the bound travel together.

    An async action gets an async wrapper. A sync wrapper around a
    coroutine function would catch only the failure of *creating* the
    coroutine, which is nothing, and the protection would read as present
    while doing nothing at all.
    """
    if asyncio.iscoroutinefunction(action):

        @functools.wraps(action)
        async def _guarded_async():
            try:
                return await action()
            except restate.TerminalError:
                raise
            except BaseException as exc:  # noqa: BLE001 -- re-raised below
                terminal = _terminal_for(exc)
                if terminal is None:
                    raise
                raise terminal from exc

        return _guarded_async

    @functools.wraps(action)
    def _guarded():
        try:
            return action()
        except restate.TerminalError:
            raise
        except BaseException as exc:  # noqa: BLE001 -- re-raised below
            terminal = _terminal_for(exc)
            if terminal is None:
                raise
            raise terminal from exc

    return _guarded


def run_options() -> Dict[str, Any]:
    """The retry bound every connecting ``ctx.run`` carries.

    Splatted into the call, the way ``api_call_plan`` does it, so a step
    that is never going to succeed stops instead of retrying for the life
    of the worker.

    Read per call rather than at import so the two environment variables
    are the same kind of knob as the rest of the worker's: a deployment
    whose server allows fewer failed logins than this default tunes it
    without a release.
    """
    options: Dict[str, Any] = {}

    raw_attempts = os.environ.get(MAX_ATTEMPTS_ENV)
    attempts = DEFAULT_MAX_ATTEMPTS
    if raw_attempts:
        try:
            attempts = int(raw_attempts)
        except ValueError:
            attempts = DEFAULT_MAX_ATTEMPTS
    # Zero or negative is never "no bound" -- unbounded is the state this
    # module exists to leave, so a nonsense value falls back to the
    # default rather than disabling the protection.
    options["max_attempts"] = attempts if attempts >= 1 else DEFAULT_MAX_ATTEMPTS

    raw_seconds = os.environ.get(MAX_RETRY_SECONDS_ENV)
    if raw_seconds:
        try:
            seconds = float(raw_seconds)
        except ValueError:
            seconds = 0.0
        if seconds > 0:
            options["max_retry_duration"] = timedelta(seconds=seconds)

    return options

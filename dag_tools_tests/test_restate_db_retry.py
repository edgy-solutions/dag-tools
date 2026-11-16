"""The seal: a wrong password causes one login attempt, not a loop.

``ctx.run`` retries whatever raises, and a failed login is not a free
operation -- every attempt spends one of the account's failed-login
attempts, so an unbounded retry of a wrong password is what locks the
account out. The lockout then takes down every other consumer of that
account, including the ones whose password was right all along.

Why this module exists separately from the handler tests: the context
double there calls the action once and returns what it gives back. Under
a fake that never retries, an unbounded step and a bounded one behave
identically, so no test written against it can see the difference this
module is about. ``RetryingContext`` honours ``max_attempts`` the way the
server does, and ``test_without_the_guard_a_wrong_password_would_loop``
is the control that proves it can observe a loop at all.
"""
import asyncio

import pytest

pytest.importorskip("restate")

import restate

from dag_tools.restate_handlers import db_retry

# What the drivers actually say, kept verbatim so the classifier is
# tested against message shapes rather than against its own constants.
WRONG_PASSWORD = "ORA-01017: invalid username/password; logon denied"
LOCKED_OUT = "ORA-28000: the account is locked"
NO_LISTENER = "ORA-12541: TNS:no listener"
SQLSERVER_LOGIN = (
    "[28000] [Microsoft][ODBC Driver 18 for SQL Server]"
    "Login failed for user \"svc_pdm\". (18456) (SQLDriverConnect)"
)


class RetryingContext:
    """A ``restate.Context`` double that actually retries.

    Retry what raises, stop at a ``TerminalError``, give up at
    ``max_attempts``. ``unbounded`` stands in for the server's real
    behaviour when no bound is given -- small enough to keep the test
    fast, large enough that a loop is unmistakable.
    """

    def __init__(self, unbounded=25):
        self.attempts = {}
        self._unbounded = unbounded

    async def run(self, name, fn, **options):
        limit = options.get("max_attempts", self._unbounded)
        last = None
        for _ in range(limit):
            self.attempts[name] = self.attempts.get(name, 0) + 1
            try:
                result = fn()
                if asyncio.iscoroutine(result):
                    result = await result
                return result
            except restate.TerminalError:
                raise
            except BaseException as exc:  # noqa: BLE001 -- re-raised below
                last = exc
        raise last


def _counting(exc_factory):
    """An action that raises, and a counter of how often it was called."""
    calls = []

    def action():
        calls.append(1)
        raise exc_factory()

    return action, calls


def _raiser(exc):
    def action():
        raise exc

    return action


# --------------------------------------------------------------------- #
# The seal
# --------------------------------------------------------------------- #

def test_a_wrong_password_causes_one_login_attempt_not_a_loop():
    ctx = RetryingContext()
    action, calls = _counting(lambda: Exception(WRONG_PASSWORD))

    with pytest.raises(restate.TerminalError):
        asyncio.run(ctx.run(
            "connect",
            db_retry.terminal_on_damage(action),
            **db_retry.run_options(),
        ))

    assert len(calls) == 1, (
        "a wrong password must cost exactly one login attempt; "
        "this made {}".format(len(calls))
    )


def test_without_the_guard_a_wrong_password_would_loop():
    """The control, without which the test above proves nothing.

    Unwrapped and unbounded, the identical failure is retried until the
    double gives up -- which is the account's failed-login budget being
    spent, one attempt at a time, by the retry itself.
    """
    ctx = RetryingContext(unbounded=25)
    action, calls = _counting(lambda: Exception(WRONG_PASSWORD))

    with pytest.raises(Exception):
        asyncio.run(ctx.run("connect", action))

    assert len(calls) == 25


def test_an_already_locked_account_is_not_hammered_further():
    ctx = RetryingContext()
    action, calls = _counting(lambda: Exception(LOCKED_OUT))

    with pytest.raises(restate.TerminalError):
        asyncio.run(ctx.run(
            "connect",
            db_retry.terminal_on_damage(action),
            **db_retry.run_options(),
        ))

    assert len(calls) == 1


def test_a_sql_server_login_failure_is_terminal_on_the_first_attempt():
    ctx = RetryingContext()
    action, calls = _counting(lambda: Exception(SQLSERVER_LOGIN))

    with pytest.raises(restate.TerminalError):
        asyncio.run(ctx.run(
            "connect",
            db_retry.terminal_on_damage(action),
            **db_retry.run_options(),
        ))

    assert len(calls) == 1


def test_an_async_action_is_guarded_too():
    """A sync wrapper around a coroutine function would guard nothing.

    It would catch only the failure of *creating* the coroutine, which
    never happens, so the protection would read as present in the diff
    and do nothing at all at runtime.
    """
    calls = []

    async def action():
        calls.append(1)
        raise Exception(WRONG_PASSWORD)

    ctx = RetryingContext()
    with pytest.raises(restate.TerminalError):
        asyncio.run(ctx.run(
            "connect",
            db_retry.terminal_on_damage(action),
            **db_retry.run_options(),
        ))

    assert len(calls) == 1


# --------------------------------------------------------------------- #
# The bound, which is the backstop classification cannot be
# --------------------------------------------------------------------- #

def test_an_unrecognised_failure_is_bounded_rather_than_endless():
    """A code list is a list of what has been seen.

    An authentication error nothing here recognises would still loop
    forever on classification alone, so the bound is what makes the
    damage finite in the case the classifier is wrong about.
    """
    ctx = RetryingContext()
    action, calls = _counting(lambda: Exception("ORA-99999: nobody has seen this"))

    with pytest.raises(Exception) as excinfo:
        asyncio.run(ctx.run(
            "connect",
            db_retry.terminal_on_damage(action),
            **db_retry.run_options(),
        ))

    assert not isinstance(excinfo.value, restate.TerminalError)
    assert len(calls) == db_retry.DEFAULT_MAX_ATTEMPTS


def test_the_bound_is_under_the_default_lockout_threshold():
    """Oracle's ``FAILED_LOGIN_ATTEMPTS`` defaults to 10.

    The bound has to leave room under it, because the unrecognised case
    above spends every attempt it is given.
    """
    assert db_retry.DEFAULT_MAX_ATTEMPTS < 10


def test_a_restarting_listener_is_retried_because_that_is_the_point():
    """``ORA-12541`` looks permanent and is not.

    A listener that is restarting answers on the next attempt, which is
    precisely the case ``ctx.run`` exists for -- so this one must NOT be
    classified, or the retry bound would have nothing left to protect.
    """
    calls = []

    def action():
        calls.append(1)
        if len(calls) == 1:
            raise Exception(NO_LISTENER)
        return "connected"

    ctx = RetryingContext()
    result = asyncio.run(ctx.run(
        "connect",
        db_retry.terminal_on_damage(action),
        **db_retry.run_options(),
    ))

    assert result == "connected"
    assert len(calls) == 2


def test_a_row_number_that_looks_like_a_login_code_is_not_one():
    """SQL Server codes are matched as standalone numbers.

    They arrive inside a sentence rather than behind a prefix, so a bare
    substring search would read the digits out of any number that
    happens to contain them.
    """
    assert db_retry.classify(Exception("row 184567 rejected by constraint")) is None
    assert db_retry.classify(Exception("affected rows: 218456")) is None


# --------------------------------------------------------------------- #
# What the message is allowed to carry
# --------------------------------------------------------------------- #

def test_the_terminal_message_carries_nothing_from_the_drivers_text():
    """A ``TerminalError``'s message reaches the journal and the caller.

    Connection errors quote the connect identifier and sometimes the
    account name, so the message is the code plus a fixed sentence and
    nothing else. The original survives as the cause, where the worker's
    own log already had it.
    """
    original = Exception(SQLSERVER_LOGIN)
    ctx = RetryingContext()

    with pytest.raises(restate.TerminalError) as excinfo:
        asyncio.run(ctx.run(
            "connect",
            db_retry.terminal_on_damage(_raiser(original)),
            **db_retry.run_options(),
        ))

    message = str(excinfo.value)
    assert "18456" in message, "the code is what makes it diagnosable"
    assert "svc_pdm" not in message
    assert "ODBC Driver" not in message
    assert excinfo.value.__cause__ is original


def test_absent_credentials_are_terminal_and_the_message_names_them():
    """The one message that does travel, because it is ours.

    It names the variables that are missing and carries no value from
    any of them, and it is the most useful thing to put in front of
    whoever reads the failed invocation.
    """
    ctx = RetryingContext()
    action, calls = _counting(
        lambda: db_retry.MissingCredentials("Missing ORACLE_DSN in environment.")
    )

    with pytest.raises(restate.TerminalError) as excinfo:
        asyncio.run(ctx.run(
            "connect",
            db_retry.terminal_on_damage(action),
            **db_retry.run_options(),
        ))

    assert "ORACLE_DSN" in str(excinfo.value)
    assert len(calls) == 1


def test_missing_credentials_stays_a_value_error():
    """AGENTS section 6: a caller catching ``ValueError`` keeps working."""
    assert issubclass(db_retry.MissingCredentials, ValueError)


def test_a_terminal_error_the_action_raised_is_not_rewrapped():
    original = restate.TerminalError("the handler decided this itself")
    ctx = RetryingContext()

    with pytest.raises(restate.TerminalError) as excinfo:
        asyncio.run(ctx.run(
            "step",
            db_retry.terminal_on_damage(_raiser(original)),
            **db_retry.run_options(),
        ))

    assert excinfo.value is original


# --------------------------------------------------------------------- #
# The bound is configuration, and nonsense never disables it
# --------------------------------------------------------------------- #

def test_the_bound_is_tunable_without_a_release(monkeypatch):
    monkeypatch.setenv(db_retry.MAX_ATTEMPTS_ENV, "2")
    assert db_retry.run_options()["max_attempts"] == 2


def test_a_nonsense_bound_falls_back_rather_than_removing_the_bound(monkeypatch):
    """Zero is never "no bound".

    Unbounded is the state this module exists to leave, so a value that
    cannot be read -- or that asks for no attempts at all -- falls back
    to the default instead of disabling the protection.
    """
    for value in ("0", "-1", "lots", "3.5"):
        monkeypatch.setenv(db_retry.MAX_ATTEMPTS_ENV, value)
        assert db_retry.run_options()["max_attempts"] == db_retry.DEFAULT_MAX_ATTEMPTS


def test_no_retry_duration_is_set_unless_one_is_configured(monkeypatch):
    monkeypatch.delenv(db_retry.MAX_RETRY_SECONDS_ENV, raising=False)
    assert "max_retry_duration" not in db_retry.run_options()
    monkeypatch.setenv(db_retry.MAX_RETRY_SECONDS_ENV, "30")
    assert db_retry.run_options()["max_retry_duration"].total_seconds() == 30


# --------------------------------------------------------------------- #
# The same seal through the real handlers, which is what was reported
# --------------------------------------------------------------------- #

def test_the_real_ack_handler_makes_one_login_attempt_on_a_wrong_password(monkeypatch):
    """The reported failure, end to end.

    Everything above exercises the guard. This drives the handler that
    was actually locking the account out, through a context that
    retries, and counts how many times the driver was asked to connect.
    """
    pytest.importorskip("oracledb")
    from dag_tools.restate_handlers import oracle_ack

    monkeypatch.setenv("ORACLE_DSN", "a-dsn")
    monkeypatch.setenv("ORACLE_USER", "a-user")
    monkeypatch.setenv("ORACLE_PASSWORD", "the-wrong-one")

    connects = []

    def refuse(*args, **kwargs):
        connects.append(1)
        raise Exception(WRONG_PASSWORD)

    monkeypatch.setattr(oracle_ack.oracledb, "connect", refuse)

    with pytest.raises(restate.TerminalError):
        asyncio.run(oracle_ack.mark_as_processed(
            RetryingContext(),
            {"table_name": "T", "pk_column": "K", "record_ids": [1]},
        ))

    assert len(connects) == 1, (
        "the handler asked the server to authenticate "
        "{} times for one wrong password".format(len(connects))
    )


def test_every_connecting_step_in_the_handlers_carries_a_bound():
    """A guard applied to four of five call sites is not a seal.

    Each of these opens a database connection, so each is a place an
    unbounded retry spends an account's failed-login budget. The ones
    deliberately left alone post to an HTTP API and connect to nothing.
    """
    import pathlib

    import dag_tools.restate_handlers as handlers

    # Read the files rather than ``inspect.getsource``: in
    # ``sap_induction.service`` the name ``service`` is bound to a
    # ``restate.Service`` instance, so the module object is shadowed.
    root = pathlib.Path(handlers.__file__).parent
    expected = {
        "oracle_ack.py": ["update_oracle"],
        "oracle_control.py": ["write_mei_request", "signal_load_complete"],
        "api_sync.py": ["update_sqlserver"],
        "sap_induction/service.py": ["local-db-update-"],
    }

    for relative, step_names in expected.items():
        source = (root / relative).read_text(encoding="utf-8")
        for step in step_names:
            # The step name as the ``ctx.run`` call spells it. Matching
            # the bare name would find the prose about it in a docstring
            # first and read the wrong 400 characters.
            quoted = source.split("\"" + step)
            assert len(quoted) > 1, (
                "{} lost its {} step".format(relative, step)
            )
            # The wrapper and the bound travel together at the call site,
            # which is what makes both halves visible in one reading.
            windows = [tail[:400] for tail in quoted[1:]]
            assert any("terminal_on_damage" in w for w in windows), (
                "{} step {} is not guarded".format(relative, step)
            )
            assert any("run_options()" in w for w in windows), (
                "{} step {} has no retry bound".format(relative, step)
            )

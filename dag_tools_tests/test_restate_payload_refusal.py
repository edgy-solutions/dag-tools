"""The second seal: a malformed payload costs one invocation, not a loop.

The lockout fix bounded every step that connects. It left one failure
outside every bound, because the refusal was outside the step: the
handler read the payload, found it unusable, and raised a plain
``ValueError`` before ``ctx.run`` was ever reached.

A step's bound bounds the step. Out at the handler, Restate retries an
invocation whose handler raised a non-terminal error, and there is no
``max_attempts`` to stop it -- so a payload with a field missing was
retried with backoff for the life of the worker, with no journal entry
naming what was wrong. Nothing was damaged, which is why it went
unnoticed; the invocation simply never ended.

``RetryingInvoker`` is what makes the difference observable. The
step-level double in ``test_restate_db_retry`` cannot see it, because
the failure this module is about happens before any step exists, and
``test_the_old_shape_would_loop_at_the_invocation_level`` is the control
that proves the invoker can observe a loop at all.
"""
import asyncio

import pytest

pytest.importorskip("restate")
pytest.importorskip("oracledb")

import restate

from dag_tools.restate_handlers import db_retry, oracle_ack

GOOD = {"table_name": "T", "pk_column": "K", "record_ids": [1, 2]}


class StepContext:
    """A ``restate.Context`` double that runs the action once."""

    def __init__(self):
        self.steps = []

    async def run(self, name, fn, **options):
        self.steps.append(name)
        result = fn()
        if asyncio.iscoroutine(result):
            result = await result
        return result


class NeverRunsTheStep:
    """A context that journals the step without executing the action.

    Which is what a replay does. Anything the handler checks *before*
    the step still runs here -- so a handler that refuses a payload
    under this context is one whose refusal is outside the bound.
    """

    async def run(self, name, fn, **options):
        return None


class RetryingInvoker:
    """What the server does with a handler that raised.

    A non-terminal error is retried; a ``restate.TerminalError`` is not.
    There is no attempt cap out here, so ``cap`` stands in for a loop
    that in production does not stop -- small enough to keep the test
    fast, large enough that a loop is unmistakable.
    """

    def __init__(self, cap=25):
        self.invocations = 0
        self._cap = cap

    def invoke(self, handler, payload, context_factory=StepContext):
        last = None
        for _ in range(self._cap):
            self.invocations += 1
            try:
                return asyncio.run(handler(context_factory(), payload))
            except restate.TerminalError:
                raise
            except BaseException as exc:  # noqa: BLE001 -- the retryable case
                last = exc
        raise last


def _refusing_driver(monkeypatch):
    """A driver that fails the test if the step ever reaches it."""
    connects = []

    def connect(*args, **kwargs):
        connects.append(1)
        raise AssertionError("the step reached the driver")

    monkeypatch.setattr(oracle_ack.oracledb, "connect", connect)
    return connects


def _credentials(monkeypatch):
    monkeypatch.setenv("ORACLE_DSN", "a-dsn")
    monkeypatch.setenv("ORACLE_USER", "a-user")
    monkeypatch.setenv("ORACLE_PASSWORD", "a-password")


# --------------------------------------------------------------------- #
# The seal
# --------------------------------------------------------------------- #

def test_a_malformed_payload_costs_one_invocation_not_a_loop(monkeypatch):
    _credentials(monkeypatch)
    connects = _refusing_driver(monkeypatch)
    invoker = RetryingInvoker()

    with pytest.raises(restate.TerminalError):
        invoker.invoke(oracle_ack.mark_as_processed, {"table_name": "T"})

    assert invoker.invocations == 1, (
        "a payload that cannot be acted on must end the invocation; "
        "this one was retried {} times".format(invoker.invocations)
    )
    assert connects == [], "a refused payload must not reach the driver"


def test_the_old_shape_would_loop_at_the_invocation_level():
    """The control, without which the test above proves nothing.

    This is what the handler did before: read the payload, refuse it
    with a plain ``ValueError``, outside any step. The invoker retries
    it until the cap -- and in production there is no cap.
    """

    async def old_shape(ctx, payload):
        if not payload.get("table_name"):
            raise ValueError("Missing required payload fields")
        return await ctx.run("update_oracle", lambda: None)

    invoker = RetryingInvoker(cap=25)
    with pytest.raises(ValueError):
        invoker.invoke(old_shape, {})

    assert invoker.invocations == 25


def test_the_refusal_happens_inside_the_step_not_before_it(monkeypatch):
    """Structural, and the reason the fix is where it is.

    Under a context that journals the step without executing it --
    which is what a replay does -- nothing the handler checks before
    the step can be reached. A handler that still refuses here is one
    whose refusal is outside the bound.
    """
    _credentials(monkeypatch)
    _refusing_driver(monkeypatch)

    asyncio.run(oracle_ack.mark_as_processed(NeverRunsTheStep(), {"nothing": "usable"}))


def test_a_payload_that_is_not_a_mapping_at_all_is_refused(monkeypatch):
    _credentials(monkeypatch)
    _refusing_driver(monkeypatch)
    invoker = RetryingInvoker()

    with pytest.raises(restate.TerminalError) as excinfo:
        invoker.invoke(oracle_ack.mark_as_processed, ["not", "a", "mapping"])

    assert invoker.invocations == 1
    assert "table_name" in str(excinfo.value)


# --------------------------------------------------------------------- #
# The failure that looked like a success
# --------------------------------------------------------------------- #

def test_a_string_of_ids_is_refused_rather_than_flipping_the_wrong_rows(monkeypatch):
    """The one malformed payload that did not loop -- it committed.

    A string is truthy, sliceable and iterable, so the old presence
    check accepted it and the chunker bound it one CHARACTER at a time:
    an UPDATE that ran, committed, and flipped whatever rows happened to
    carry those single-character keys.
    """
    _credentials(monkeypatch)
    connects = _refusing_driver(monkeypatch)

    with pytest.raises(restate.TerminalError) as excinfo:
        asyncio.run(oracle_ack.mark_as_processed(
            StepContext(), dict(GOOD, record_ids="12345"),
        ))

    assert connects == [], "nothing may be committed from a string of ids"
    message = str(excinfo.value)
    assert "record_ids" in message
    assert "string" in message, (
        "the refusal must say what was wrong, not just that something was"
    )


def test_an_empty_list_of_ids_is_refused_as_it_always_was(monkeypatch):
    """Unchanged behaviour, named so it stays unchanged.

    An empty list is a different problem from a string -- there is
    nothing silently wrong about it, it is just a request to do nothing
    -- and it was refused before this change too.
    """
    _credentials(monkeypatch)
    _refusing_driver(monkeypatch)

    with pytest.raises(restate.TerminalError) as excinfo:
        asyncio.run(oracle_ack.mark_as_processed(
            StepContext(), dict(GOOD, record_ids=[]),
        ))

    assert "record_ids" in str(excinfo.value)


# --------------------------------------------------------------------- #
# What the refusal is allowed to carry
# --------------------------------------------------------------------- #

def test_the_refusal_carries_no_value_from_the_payload(monkeypatch):
    """The message reaches the journal and the caller.

    It is ours rather than a driver's, so it travels -- which makes it
    this handler's job to name the fields and nothing else. A payload
    names a table and a column, and those are names from below.
    """
    _credentials(monkeypatch)
    _refusing_driver(monkeypatch)

    planted = {
        "table_name": ["PLANTED_TABLE"],
        "pk_column": ["PLANTED_COLUMN"],
        "record_ids": "PLANTED_IDS",
        "stats_table": ["PLANTED_STATS_TABLE"],
        "stats_columns": ["PLANTED_STATS_COLUMN"],
    }

    with pytest.raises(restate.TerminalError) as excinfo:
        asyncio.run(oracle_ack.mark_as_processed(StepContext(), planted))

    message = str(excinfo.value)
    for field in planted:
        assert field in message, "the field it refused is what makes it fixable"
    for value in ("PLANTED_TABLE", "PLANTED_COLUMN", "PLANTED_IDS",
                  "PLANTED_STATS_TABLE", "PLANTED_STATS_COLUMN"):
        assert value not in message, (
            "{} travelled into the journal".format(value)
        )


def test_every_problem_is_reported_at_once(monkeypatch):
    """Whoever is fixing the producer wants the list, not the first item."""
    _credentials(monkeypatch)
    _refusing_driver(monkeypatch)

    with pytest.raises(restate.TerminalError) as excinfo:
        asyncio.run(oracle_ack.mark_as_processed(
            StepContext(), {"record_ids": []},
        ))

    message = str(excinfo.value)
    assert "table_name" in message
    assert "pk_column" in message
    assert "record_ids" in message


def test_a_malformed_payload_stays_a_value_error():
    """AGENTS section 6: a caller catching ``ValueError`` keeps working."""
    assert issubclass(db_retry.MalformedPayload, ValueError)


def test_the_classifier_calls_it_permanent_rather_than_damaging():
    """It spends nothing. It just cannot succeed.

    The two tiers exist because the reasoning differs: a wrong password
    consumes an account's failed-login budget, while a malformed payload
    consumes only attempts. Both are terminal on the first failure; only
    one of them is an outage waiting to happen.
    """
    verdict = db_retry.classify(db_retry.MalformedPayload("a field is missing"))
    assert verdict is not None
    code, _tier = verdict
    assert code == db_retry.PAYLOAD_MALFORMED
    assert code not in db_retry.DAMAGING_ORACLE_CODES


# --------------------------------------------------------------------- #
# The payload that is fine still works exactly as it did
# --------------------------------------------------------------------- #

class _Cursor:
    def __init__(self, sink):
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, binds):
        self._sink.append((sql, binds))


class _Connection:
    def __init__(self, sink, commits):
        self._sink = sink
        self._commits = commits

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return _Cursor(self._sink)

    def commit(self):
        self._commits.append(1)


def _recording_driver(monkeypatch):
    executed, commits = [], []
    monkeypatch.setattr(
        oracle_ack.oracledb,
        "connect",
        lambda **kwargs: _Connection(executed, commits),
    )
    return executed, commits


def test_a_well_formed_payload_still_updates_and_commits(monkeypatch):
    """The control for the whole change: the happy path is untouched."""
    _credentials(monkeypatch)
    executed, commits = _recording_driver(monkeypatch)

    asyncio.run(oracle_ack.mark_as_processed(
        StepContext(), {"table_name": "T", "pk_column": "K", "record_ids": [7, 8]},
    ))

    assert commits == [1]
    assert len(executed) == 1
    sql, binds = executed[0]
    assert "UPDATE T" in sql
    assert "K IN (:id0,:id1)" in sql
    assert binds == {"id0": 7, "id1": 8}


def test_a_tuple_of_ids_is_accepted_as_a_list_is(monkeypatch):
    """JSON gives a list; a Python caller may give a tuple.

    Both are sequences of identifiers, and refusing one of them would
    be a new refusal of something that already worked.
    """
    _credentials(monkeypatch)
    executed, _commits = _recording_driver(monkeypatch)

    asyncio.run(oracle_ack.mark_as_processed(
        StepContext(), {"table_name": "T", "pk_column": "K", "record_ids": (7,)},
    ))

    assert [binds for _sql, binds in executed] == [{"id0": 7}]


def test_the_stats_row_is_still_written_when_one_is_asked_for(monkeypatch):
    """The optional half of the payload still reaches the same INSERT."""
    _credentials(monkeypatch)
    executed, commits = _recording_driver(monkeypatch)

    asyncio.run(oracle_ack.mark_as_processed(StepContext(), {
        "table_name": "T",
        "pk_column": "K",
        "record_ids": [1],
        "stats_table": "S",
        "stats_columns": {"batch": "b1"},
    }))

    assert commits == [1]
    inserts = [(sql, binds) for sql, binds in executed if "INSERT INTO" in sql]
    assert len(inserts) == 1
    sql, binds = inserts[0]
    assert "INSERT INTO S" in sql
    assert binds == {"rows_acked": 1, "source_table": "T", "batch": "b1"}

"""The api_sync half of the payload seal, and the guard its HTTP step needed.

``oracle_ack`` was converted first and ``test_restate_payload_refusal``
holds that seal. This module is the same invariant on the handler whose
shape differs in the one way that matters: it reads **one** payload for
**two** steps, and the first of them does not connect to anything.

A step's bound bounds the step. Read before ``ctx.run``, as this handler
did, a refusal leaves every bound behind: the *invocation* fails, and
Restate retries an invocation that failed on a non-terminal error with
no ``max_attempts`` anywhere -- so a payload with a field missing was
retried with backoff for the life of the worker and the journal never
said what was wrong.

Two things here are not in the oracle_ack seal.

*Each step refuses for itself.* The two steps are separate journal
entries and a replay can execute one without executing the other, so a
reading done by the first is not available to the second.

*The HTTP step takes ``terminal_on_refusal``, not ``terminal_on_damage``.*
The damage guard reads the driver's error codes, and
``DAMAGING_SQLSERVER_CODES`` are matched as standalone numbers anywhere
in the exception text. An HTTP failure quotes the URL it failed on, so a
``404 ... /orders/18456`` would be read as a login failure and the call
refused instead of retried. That hazard is sealed here, with the control
that proves it is real.
"""
import asyncio

import pytest

pytest.importorskip("restate")
pytest.importorskip("oracledb")
pytest.importorskip("httpx")
requests = pytest.importorskip("requests")
pyodbc = pytest.importorskip("pyodbc")

import restate

from dag_tools.restate_handlers import api_sync, db_retry

GOOD = {
    "api_path": "/a/route",
    "source_table": "T",
    "pk_column": "K",
    "pk_value": 7,
    "row_data": {"a_column": 1},
}

#: An HTTP failure quoting a five-digit identifier in its URL. The
#: number is one of ``DAMAGING_SQLSERVER_CODES``, which is the whole
#: point: nothing about this failure is a login failure.
HTTP_FAILURE_QUOTING_A_CODE = (
    "404 Client Error: Not Found for url: http://api.invalid/orders/18456"
)


# --------------------------------------------------------------------- #
# Doubles
# --------------------------------------------------------------------- #

class StepContext:
    """A ``restate.Context`` double that runs each action once.

    It also records the options each step was dispatched with, which is
    how the bound on one step and the deliberate absence of a bound on
    the other are both observable.
    """

    def __init__(self):
        self.steps = []
        self.options = {}
        self.state = {}

    async def run(self, name, fn, **options):
        self.steps.append(name)
        self.options[name] = options
        result = fn()
        if asyncio.iscoroutine(result):
            result = await result
        return result

    def set(self, key, value):
        self.state[key] = value


class NeverRunsTheStep:
    """A context that journals each step without executing its action.

    Which is what a replay does. Anything the handler checks *before* a
    step still runs here -- so a handler that refuses a payload under
    this context is one whose refusal is outside the bound.
    """

    async def run(self, name, fn, **options):
        return None

    def set(self, key, value):
        pass


class ReplaysTheFirstStep:
    """A context that returns step one from the journal and runs step two.

    The state a replay is actually in. The first action is never called,
    so a second step relying on a reading the first one did is a second
    step about to act on nothing.
    """

    def __init__(self, banked="BANKED_ID"):
        self.steps = []
        self.state = {}
        self._banked = banked

    async def run(self, name, fn, **options):
        self.steps.append(name)
        if len(self.steps) == 1:
            return self._banked
        result = fn()
        if asyncio.iscoroutine(result):
            result = await result
        return result

    def set(self, key, value):
        self.state[key] = value


class RetryingInvoker:
    """What the server does with a handler that raised.

    A non-terminal error is retried; a ``restate.TerminalError`` is not.
    There is no attempt cap out here, so ``cap`` stands in for a loop
    that in production does not stop -- small enough to keep the test
    fast, large enough that a loop is unmistakable.
    """

    def __init__(self, cap=25):
        self.invocations = 0
        self.cap = cap

    def invoke(self, handler, payload, context_factory=StepContext):
        last = None
        for _ in range(self.cap):
            self.invocations += 1
            try:
                return asyncio.run(handler(context_factory(), payload))
            except restate.TerminalError:
                raise
            except BaseException as exc:  # noqa: BLE001 -- the retryable case
                last = exc
        raise last


class _Response:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        return None

    def json(self):
        return self._body


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("API_BASE_URL", "http://api.invalid/")
    monkeypatch.setenv("API_KEY", "a-key")
    monkeypatch.setenv("SQLSERVER_DSN", "a-dsn")


@pytest.fixture
def refusing_transport(monkeypatch):
    """Transports that fail the test if either step reaches them."""

    def post(*args, **kwargs):
        raise AssertionError("the step reached the API")

    def connect(*args, **kwargs):
        raise AssertionError("the step reached the database")

    monkeypatch.setattr(requests, "post", post)
    monkeypatch.setattr(pyodbc, "connect", connect)


# --------------------------------------------------------------------- #
# The seal
# --------------------------------------------------------------------- #

def test_a_malformed_payload_costs_one_invocation_not_a_loop(
    env, refusing_transport
):
    invoker = RetryingInvoker()

    with pytest.raises(restate.TerminalError) as caught:
        invoker.invoke(api_sync.process_record, dict(GOOD, pk_value=None))

    assert invoker.invocations == 1
    assert "pk_value" in str(caught.value)


def test_control_the_old_shape_would_loop_at_the_invocation_level():
    """The invoker can observe a loop, so the test above means something.

    This is the shape the handler had: the payload read, and refused,
    before any step exists. Nothing bounds it.
    """

    async def old_shape(ctx, payload):
        if not payload.get("pk_value"):
            raise ValueError("Missing required payload fields: pk_value")
        await ctx.run("sap_api_post", lambda: None)

    invoker = RetryingInvoker()

    with pytest.raises(ValueError):
        invoker.invoke(old_shape, dict(GOOD, pk_value=None))

    assert invoker.invocations == invoker.cap


def test_the_refusal_happens_inside_the_step_not_before_it(env):
    """Under a context that executes no action, nothing is refused.

    If the handler read its payload before ``ctx.run`` this would raise
    here, which is exactly the defect: a refusal reachable without the
    step is a refusal outside the bound of the step.
    """
    asyncio.run(api_sync.process_record(NeverRunsTheStep(), {"nothing": "usable"}))


def test_each_step_refuses_for_itself_because_a_replay_can_skip_the_first(
    env, monkeypatch
):
    """The reason ``_validated`` is called twice rather than once.

    A replay returns the first step from the journal without calling its
    action, so a reading the first step did is not available to the
    second. Here the first step is banked and the second still refuses.
    """

    def connect(*args, **kwargs):
        raise AssertionError("the step reached the database")

    monkeypatch.setattr(pyodbc, "connect", connect)
    ctx = ReplaysTheFirstStep()

    with pytest.raises(restate.TerminalError) as caught:
        asyncio.run(api_sync.process_record(ctx, dict(GOOD, pk_column=None)))

    assert ctx.steps == ["sap_api_post", "update_sqlserver"]
    assert "pk_column" in str(caught.value)


def test_a_string_of_row_data_is_refused_rather_than_posted_as_a_bare_string(
    env, refusing_transport
):
    """The dangerous shape, and the reason it is named apart.

    A string is valid JSON, so the request would be built, sent and
    quite possibly accepted -- a row posted to the API as a bare string.
    It is the failure here that reads as a success, so it does not share
    a message with the shapes that merely refuse.
    """
    with pytest.raises(restate.TerminalError, match="a string is not one") as caught:
        asyncio.run(
            api_sync.process_record(StepContext(), dict(GOOD, row_data="a_value"))
        )

    assert "the API may accept" in str(caught.value)


def test_a_path_that_would_extend_the_host_is_refused(env, refusing_transport):
    """A path with no leading slash joins the base URL as a host.

    The base URL has its trailing slash stripped, so the result is a
    different host rather than a different route -- and that failure
    looks transient, on a step that is deliberately unbounded.
    """
    with pytest.raises(restate.TerminalError, match="api_path") as caught:
        asyncio.run(
            api_sync.process_record(StepContext(), dict(GOOD, api_path="a/route"))
        )

    assert "host" in str(caught.value)


def test_the_refusal_carries_no_value_from_the_payload(env, refusing_transport):
    """The message travels into the journal and back to the caller.

    A payload for this handler is nothing but the names of a table, a
    column, a route and a key. So the refusal names the fields it
    refused and no value from any of them.
    """
    planted = {
        "api_path": "/a-planted-route",
        "source_table": "A_PLANTED_TABLE",
        "pk_column": "A_PLANTED_COLUMN",
        "pk_value": ["a-planted-key"],
        "row_data": {"A_PLANTED_CELL_COLUMN": "a-planted-cell"},
    }

    with pytest.raises(restate.TerminalError) as caught:
        asyncio.run(api_sync.process_record(StepContext(), planted))

    message = str(caught.value)
    assert "pk_value" in message
    for value in (
        "a-planted-route",
        "A_PLANTED_TABLE",
        "A_PLANTED_COLUMN",
        "a-planted-key",
        "A_PLANTED_CELL_COLUMN",
        "a-planted-cell",
    ):
        assert value not in message


def test_every_problem_is_reported_at_once(env, refusing_transport):
    """Whoever is fixing the producer wants the list, not the first one."""
    with pytest.raises(restate.TerminalError) as caught:
        asyncio.run(
            api_sync.process_record(
                StepContext(),
                {"source_table": "T", "pk_column": "K", "row_data": "a_value"},
            )
        )

    message = str(caught.value)
    assert "api_path" in message
    assert "pk_value" in message
    assert "row_data" in message


def test_a_payload_that_is_not_a_mapping_at_all_is_refused(env, refusing_transport):
    with pytest.raises(restate.TerminalError, match="not a mapping"):
        asyncio.run(api_sync.process_record(StepContext(), ["not", "a", "mapping"]))


# --------------------------------------------------------------------- #
# Absent credentials, on a step that has no bound
# --------------------------------------------------------------------- #

def test_an_absent_base_url_is_terminal_rather_than_retried_forever(
    env, monkeypatch
):
    """``API_BASE_URL`` is read from the environment, which is a snapshot.

    ``os.environ`` is populated once, when the process starts, so a
    variable absent now stays absent for the life of the pod. Raised as
    a plain ``ValueError``, as it was, this step retried it for exactly
    that long: it carries no ``max_attempts``, on purpose.
    """
    monkeypatch.delenv("API_BASE_URL", raising=False)

    def post(*args, **kwargs):
        raise AssertionError("the step reached the API")

    monkeypatch.setattr(requests, "post", post)
    invoker = RetryingInvoker()

    with pytest.raises(restate.TerminalError) as caught:
        invoker.invoke(api_sync.process_record, dict(GOOD))

    assert invoker.invocations == 1
    assert "API_BASE_URL" in str(caught.value)


def test_an_absent_dsn_is_terminal_on_the_acking_step_too(env, monkeypatch):
    monkeypatch.delenv("SQLSERVER_DSN", raising=False)
    monkeypatch.setattr(
        requests, "post", lambda *a, **k: _Response({"id": "AN_ID"})
    )

    def connect(*args, **kwargs):
        raise AssertionError("the step reached the database")

    monkeypatch.setattr(pyodbc, "connect", connect)

    with pytest.raises(restate.TerminalError) as caught:
        asyncio.run(api_sync.process_record(StepContext(), dict(GOOD)))

    assert "SQLSERVER_DSN" in str(caught.value)


# --------------------------------------------------------------------- #
# The guard the HTTP step needed, and the hazard it exists for
# --------------------------------------------------------------------- #

def test_an_http_failure_quoting_a_five_digit_id_is_retried_not_refused(
    env, monkeypatch
):
    """Why this step does not take ``terminal_on_damage``.

    The damage guard matches ``DAMAGING_SQLSERVER_CODES`` as standalone
    numbers anywhere in the exception text, because a driver puts its
    vendor code there and nowhere reliable. An HTTP failure quotes the
    URL it failed on -- and a route carrying a five-digit identifier is
    an ordinary thing for a URL to carry. Under the damage guard that
    ``404`` would be read as a login failure and the call abandoned on
    the first attempt. Retrying it is the whole reason the step exists.
    """
    failure = requests.exceptions.HTTPError(HTTP_FAILURE_QUOTING_A_CODE)

    def post(*args, **kwargs):
        raise failure

    monkeypatch.setattr(requests, "post", post)

    with pytest.raises(requests.exceptions.HTTPError) as caught:
        asyncio.run(api_sync.process_record(StepContext(), dict(GOOD)))

    assert caught.value is failure


def test_control_terminal_on_damage_would_have_refused_that_same_failure():
    """Without this, the test above would pass on any guard at all."""
    failure = requests.exceptions.HTTPError(HTTP_FAILURE_QUOTING_A_CODE)

    def action():
        raise failure

    with pytest.raises(restate.TerminalError) as caught:
        db_retry.terminal_on_damage(action)()

    assert "18456" in str(caught.value)


def test_the_refusal_guard_makes_our_own_refusal_terminal():
    def action():
        raise db_retry.MalformedPayload("The payload cannot be acted on: x.")

    with pytest.raises(restate.TerminalError) as caught:
        db_retry.terminal_on_refusal(action)()

    assert "cannot be acted on" in str(caught.value)
    assert isinstance(caught.value.__cause__, db_retry.MalformedPayload)


def test_the_refusal_guard_leaves_every_other_failure_alone():
    """Including one whose text carries a code the damage guard reads.

    The refusal travels and the classification does not: that asymmetry
    is the guard.
    """
    for failure in (
        TimeoutError("the connection timed out"),
        requests.exceptions.HTTPError(HTTP_FAILURE_QUOTING_A_CODE),
    ):

        def action(failure=failure):
            raise failure

        with pytest.raises(type(failure)) as caught:
            db_retry.terminal_on_refusal(action)()

        assert caught.value is failure


def test_an_async_action_is_guarded_too():
    """A sync wrapper around a coroutine function protects nothing.

    It would catch only the failure of *creating* the coroutine, which
    never happens, so the protection would read as present and do
    nothing at all.
    """

    async def action():
        raise db_retry.MissingCredentials("Missing A_VARIABLE in environment.")

    with pytest.raises(restate.TerminalError) as caught:
        asyncio.run(db_retry.terminal_on_refusal(action)())

    assert "A_VARIABLE" in str(caught.value)


# --------------------------------------------------------------------- #
# The happy path, and the bound that is there against the one that is not
# --------------------------------------------------------------------- #

@pytest.fixture
def recording_transport(monkeypatch):
    """Doubles that record what each step actually did."""
    posts = []
    statements = []
    commits = []

    def post(url, json=None, headers=None):
        posts.append({"url": url, "json": json, "headers": headers})
        return _Response({"id": "AN_UPSTREAM_ID"})

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, *binds):
            statements.append((sql, binds))

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def cursor(self):
            return _Cursor()

        def commit(self):
            commits.append(1)

    monkeypatch.setattr(requests, "post", post)
    monkeypatch.setattr(pyodbc, "connect", lambda dsn: _Connection())
    return {"posts": posts, "statements": statements, "commits": commits}


def test_a_well_formed_payload_still_posts_acks_and_correlates(
    env, recording_transport
):
    ctx = StepContext()

    asyncio.run(api_sync.process_record(ctx, dict(GOOD)))

    post = recording_transport["posts"][0]
    assert post["url"] == "http://api.invalid/a/route"
    assert post["json"] == {"a_column": 1}
    assert post["headers"]["Authorization"] == "Bearer a-key"

    _, binds = recording_transport["statements"][0]
    assert binds == ("AN_UPSTREAM_ID", 7)
    assert recording_transport["commits"] == [1]
    assert ctx.state == {"last_sync_7": "AN_UPSTREAM_ID"}


def test_the_acking_step_carries_a_bound_and_the_http_step_does_not(
    env, recording_transport
):
    """Both halves, in one reading.

    The acking step connects, so it is bounded: a wrong password must
    cost one login attempt and not the whole budget of the account. The
    HTTP step does not connect, and bounding it would abandon the
    transient failures ``ctx.run`` exists for.
    """
    ctx = StepContext()

    asyncio.run(api_sync.process_record(ctx, dict(GOOD)))

    assert ctx.steps == ["sap_api_post", "update_sqlserver"]
    assert ctx.options["sap_api_post"] == {}
    assert ctx.options["update_sqlserver"]["max_attempts"] >= 1

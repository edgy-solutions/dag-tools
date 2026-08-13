"""Durable-executor invariants for the API call plan handler.

The handler is exercised through a stub context rather than a live
Restate server: everything under test here is the classification and
fallback logic that sits *around* ``ctx.run``, which is exactly where a
plausible implementation goes wrong.
"""
import asyncio
import json
from typing import Any, Dict, List, Optional

import pytest
import restate

from dag_tools.restate_handlers import api_call_plan
from dag_tools.restate_handlers.api_call_plan import execute_plan


class StubContext:
    """Minimal stand-in for restate.ObjectContext.

    ``run`` invokes the closure directly and lets exceptions propagate —
    which is the point: in real Restate an exception means "retry", so a
    test that swallowed it would hide the bug this file exists to catch.
    """

    def __init__(self, key: str = "group-1", state: Optional[Dict[str, Any]] = None):
        self._key = key
        self.state: Dict[str, Any] = dict(state or {})
        self.step_names: List[str] = []

    def key(self) -> str:
        return self._key

    async def get(self, name: str):
        return self.state.get(name)

    def set(self, name: str, value: Any) -> None:
        self.state[name] = value

    async def run(self, name: str, action, **kwargs):
        self.step_names.append(name)
        return action()


class FakeHttp:
    """Records requests and replays a scripted status per (method, path)."""

    def __init__(self, responses: Dict[str, int], default: int = 200):
        self.responses = responses
        self.default = default
        self.calls: List[Dict[str, Any]] = []

    def request(self, method, url, json=None, headers=None, timeout=None):
        path = url.split("https://api.test", 1)[-1]
        status = self.responses.get(f"{method} {path}", self.default)
        self.calls.append(
            {"method": method, "url": url, "path": path, "body": json, "headers": headers or {}}
        )
        return FakeResponse(status)


class FakeResponse:
    def __init__(self, status_code: int, payload: Any = None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"ok": status_code < 300}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload


@pytest.fixture
def http(monkeypatch):
    holder: Dict[str, FakeHttp] = {}

    def install(responses=None, default=200):
        fake = FakeHttp(responses or {}, default)
        holder["fake"] = fake
        import requests

        monkeypatch.setattr(requests, "request", fake.request)
        return fake

    return install


def _plan(steps, **overrides):
    plan = {
        "format_version": api_call_plan.PLAN_FORMAT_VERSION,
        "plan_id": "group-1:hash1",
        "group_key": "group-1",
        "plan_hash": "hash1",
        "api": {
            "base_url": "https://api.test",
            "base_url_env": None,
            "headers": {},
            "header_env": {},
            "timeout_seconds": 5,
            "retry_statuses": [429, 500, 502, 503, 504],
        },
        "steps": steps,
    }
    plan.update(overrides)
    return plan


def _call(item_key, path, body=None, on_status=None, fragments=None, method="POST"):
    return {
        "item_key": item_key,
        "method": method,
        "path": path,
        "headers": {},
        "body": body,
        "on_status": on_status or {},
        "fragments": fragments or {},
    }


def _run(ctx, plan):
    return asyncio.run(execute_plan(ctx, plan))


# --- happy path ------------------------------------------------------------


def test_calls_execute_in_declared_order(http):
    fake = http()
    plan = _plan(
        [
            {"id": "one", "calls": [_call("single", "/a")], "aggregate_fallbacks": []},
            {"id": "two", "calls": [_call("single", "/b")], "aggregate_fallbacks": []},
            {"id": "three", "calls": [_call("single", "/c")], "aggregate_fallbacks": []},
        ]
    )
    result = _run(StubContext(), plan)

    assert [c["path"] for c in fake.calls] == ["/a", "/b", "/c"]
    assert result["status"] == "COMPLETED"
    assert result["calls_executed"] == 3


def test_durable_step_names_are_unique_and_include_the_fanout_item(http):
    http()
    ctx = StubContext()
    plan = _plan(
        [
            {
                "id": "record",
                "calls": [_call("span-1", "/r/1"), _call("span-2", "/r/2")],
                "aggregate_fallbacks": [],
            }
        ]
    )
    _run(ctx, plan)

    assert ctx.step_names == ["00-record-span-1", "00-record-span-2"]
    assert len(set(ctx.step_names)) == len(ctx.step_names)


# --- status classification (the retry-semantics trap) ----------------------


def test_retryable_status_raises_so_restate_retries(http):
    """5xx must propagate. Swallowing it would strand the group silently."""
    http({"POST /a": 503})
    with pytest.raises(RuntimeError, match="retryable"):
        _run(ctx := StubContext(), _plan([{"id": "s", "calls": [_call("single", "/a")], "aggregate_fallbacks": []}]))
    assert ctx.step_names == ["00-s-single"]


def test_unhandled_client_error_is_terminal_not_retried(http):
    """A 400 cannot be fixed by retrying; it must fail terminally."""
    http({"POST /a": 400})
    with pytest.raises(restate.TerminalError):
        _run(StubContext(), _plan([{"id": "s", "calls": [_call("single", "/a")], "aggregate_fallbacks": []}]))


def test_status_with_a_fallback_does_not_raise(http):
    """The whole reason status is returned as data rather than raised.

    If the HTTP layer raised on non-2xx, this 404 would be retried
    forever and the fallback would never run.
    """
    fake = http({"PATCH /entity/e1": 404})
    ctx = StubContext()
    plan = _plan(
        [
            {
                "id": "entity",
                "calls": [
                    _call(
                        "e1",
                        "/entity/e1",
                        method="PATCH",
                        body={"artifacts": "f1"},
                        on_status={
                            "404": {
                                "method": "POST",
                                "path": "/entity",
                                "headers": {},
                                "body": {"entities": [{"entityIdentifier": "e1"}]},
                            }
                        },
                    )
                ],
                "aggregate_fallbacks": [],
            }
        ]
    )
    result = _run(ctx, plan)

    assert [c["path"] for c in fake.calls] == ["/entity/e1", "/entity"]
    assert result["fallbacks_run"] == 1
    assert result["status"] == "COMPLETED"
    assert "00-entity-e1-fallback-404" in ctx.step_names


def test_continue_on_error_records_the_failure_without_aborting(http):
    http({"POST /a": 400})
    result = _run(
        StubContext(),
        _plan(
            [
                {
                    "id": "s",
                    "continue_on_error": True,
                    "calls": [_call("x", "/a"), _call("y", "/b")],
                    "aggregate_fallbacks": [],
                }
            ]
        ),
    )
    assert result["status"] == "COMPLETED_WITH_ERRORS"
    assert result["failures"][0]["item"] == "x"
    assert result["calls_executed"] == 2


# --- aggregate fallback blast radius --------------------------------------


AGGREGATE_STEP = {
    "id": "entity_artifacts",
    "calls": [
        _call(
            "e1",
            "/entity/e1",
            method="PATCH",
            fragments={"404": {"entityIdentifier": "e1", "artifacts": "f1.txt"}},
        ),
        _call(
            "e2",
            "/entity/e2",
            method="PATCH",
            fragments={"404": {"entityIdentifier": "e2", "artifacts": "f2.txt"}},
        ),
        _call(
            "e3",
            "/entity/e3",
            method="PATCH",
            fragments={"404": {"entityIdentifier": "e3", "artifacts": "f3.txt"}},
        ),
    ],
    "aggregate_fallbacks": [
        {
            "status": 404,
            "method": "POST",
            "path": "/entity",
            "headers": {},
            "body": {"deleteMissingEntities": False, "entities": []},
            "collect_into": "entities",
        }
    ],
}


def test_aggregate_fallback_sends_only_the_items_that_failed(http):
    """The destructive case: the bulk create must not cover successes.

    e2 404s; e1 and e3 succeed. The bulk endpoint has replace semantics,
    so including e1/e3 would overwrite server-side state that the
    telemetry never contained and cannot reconstruct.
    """
    fake = http({"PATCH /entity/e2": 404})
    result = _run(StubContext(), _plan([AGGREGATE_STEP]))

    aggregate_calls = [c for c in fake.calls if c["path"] == "/entity"]
    assert len(aggregate_calls) == 1
    entities = aggregate_calls[0]["body"]["entities"]
    assert [e["entityIdentifier"] for e in entities] == ["e2"]
    assert aggregate_calls[0]["body"]["deleteMissingEntities"] is False
    assert result["fallbacks_run"] == 1


def test_aggregate_fallback_is_skipped_entirely_when_nothing_failed(http):
    fake = http()
    _run(StubContext(), _plan([AGGREGATE_STEP]))
    assert [c["path"] for c in fake.calls] == ["/entity/e1", "/entity/e2", "/entity/e3"]


def test_aggregate_fallback_runs_once_after_the_whole_fanout(http):
    """Ordering matters: collect across the fan-out, then send once."""
    fake = http({"PATCH /entity/e1": 404, "PATCH /entity/e3": 404})
    _run(StubContext(), _plan([AGGREGATE_STEP]))

    assert [c["path"] for c in fake.calls] == [
        "/entity/e1",
        "/entity/e2",
        "/entity/e3",
        "/entity",
    ]
    entities = fake.calls[-1]["body"]["entities"]
    assert [e["entityIdentifier"] for e in entities] == ["e1", "e3"]


# --- cross-dispatch idempotency -------------------------------------------


def test_a_previously_completed_plan_hash_is_not_re_executed(http):
    """Keying serialises; it does not deduplicate. This does.

    Without it, a re-dispatch of a still-filling group re-runs
    append-shaped calls and duplicates execution records.
    """
    fake = http()
    ctx = StubContext(state={"completed_plan_hashes": ["hash1"]})
    result = _run(ctx, _plan([{"id": "s", "calls": [_call("single", "/a")], "aggregate_fallbacks": []}]))

    assert result["status"] == "SKIPPED_DUPLICATE"
    assert fake.calls == []


def test_a_changed_plan_for_the_same_group_still_executes(http):
    fake = http()
    ctx = StubContext(state={"completed_plan_hashes": ["hash-old"]})
    _run(ctx, _plan([{"id": "s", "calls": [_call("single", "/a")], "aggregate_fallbacks": []}]))

    assert [c["path"] for c in fake.calls] == ["/a"]
    assert ctx.state["completed_plan_hashes"] == ["hash-old", "hash1"]


def test_completed_hash_history_is_bounded(http):
    http()
    ctx = StubContext(state={"completed_plan_hashes": [f"h{i}" for i in range(80)]})
    _run(ctx, _plan([{"id": "s", "calls": [_call("single", "/a")], "aggregate_fallbacks": []}]))
    assert len(ctx.state["completed_plan_hashes"]) == api_call_plan._COMPLETED_HISTORY


# --- credentials -----------------------------------------------------------


def test_auth_headers_are_expanded_from_the_workers_environment(http, monkeypatch):
    """Secrets resolve here, never in the plan that crosses the ingress."""
    monkeypatch.setenv("TEST_API_TOKEN", "s3cret")
    monkeypatch.setenv("TEST_API_BASE", "https://api.test")
    fake = http()

    plan = _plan([{"id": "s", "calls": [_call("single", "/a")], "aggregate_fallbacks": []}])
    plan["api"]["base_url"] = None
    plan["api"]["base_url_env"] = "TEST_API_BASE"
    plan["api"]["header_env"] = {"Authorization": "Bearer ${TEST_API_TOKEN}"}

    _run(StubContext(), plan)

    assert fake.calls[0]["headers"]["Authorization"] == "Bearer s3cret"
    # The plan itself still carries only the reference.
    assert plan["api"]["header_env"]["Authorization"] == "Bearer ${TEST_API_TOKEN}"


def test_a_missing_credential_fails_terminally_with_a_named_variable(http):
    http()
    plan = _plan([{"id": "s", "calls": [_call("single", "/a")], "aggregate_fallbacks": []}])
    plan["api"]["header_env"] = {"Authorization": "Bearer ${DEFINITELY_NOT_SET_TOKEN}"}

    with pytest.raises(restate.TerminalError, match="DEFINITELY_NOT_SET_TOKEN"):
        _run(StubContext(), plan)


# --- wire contract ---------------------------------------------------------


def test_an_unknown_plan_format_version_is_refused_not_partially_executed(http):
    fake = http()
    plan = _plan([{"id": "s", "calls": [_call("single", "/a")], "aggregate_fallbacks": []}])
    plan["format_version"] = 99

    with pytest.raises(restate.TerminalError, match="format_version"):
        _run(StubContext(), plan)
    assert fake.calls == []


# --- overlapping re-dispatch (late-arriving rows) --------------------------


def _keyed_call(item_key, path, key, body=None, dedupe=True):
    call = _call(item_key, path, body=body)
    call["call_key"] = key
    call["dedupe"] = dedupe
    return call


def test_a_superset_re_dispatch_only_sends_calls_not_already_delivered(http):
    """The remaining duplication window, closed.

    A group force-dispatches, late rows arrive, and the new plan
    legitimately covers the earlier rows too — a different plan hash, so
    the plan-level check cannot see it. Upserts tolerate the repeat;
    append-style calls would duplicate. Per-call identity catches it.
    """
    fake = http()
    ctx = StubContext()

    first = _plan([{"id": "record", "aggregate_fallbacks": [],
                    "calls": [_keyed_call("row-1", "/api/Record/1", "k1")]}])
    _run(ctx, first)
    assert [c["path"] for c in fake.calls] == ["/api/Record/1"]

    # Later dispatch: same row 1, plus a newly arrived row 2.
    second = _plan(
        [{"id": "record", "aggregate_fallbacks": [],
          "calls": [_keyed_call("row-1", "/api/Record/1", "k1"),
                    _keyed_call("row-2", "/api/Record/2", "k2")]}],
        plan_hash="hash2",
    )
    result = _run(ctx, second)

    assert [c["path"] for c in fake.calls] == ["/api/Record/1", "/api/Record/2"]
    assert result["calls_executed"] == 1
    assert result["calls_skipped_already_delivered"] == 1


def test_a_call_whose_body_changed_is_delivered_again(http):
    """Suppression is by content, so a genuine update still goes out."""
    fake = http()
    ctx = StubContext()

    _run(ctx, _plan([{"id": "e", "aggregate_fallbacks": [],
                      "calls": [_keyed_call("e1", "/api/E/e1", "k-old", {"artifacts": "a"})]}]))
    _run(ctx, _plan([{"id": "e", "aggregate_fallbacks": [],
                      "calls": [_keyed_call("e1", "/api/E/e1", "k-new", {"artifacts": "a,b"})]}],
                    plan_hash="hash2"))

    assert [c["body"] for c in fake.calls] == [{"artifacts": "a"}, {"artifacts": "a,b"}]


def test_a_failed_call_is_not_recorded_as_delivered(http):
    """Only success counts, or a transient failure would be skipped forever."""
    fake = http({"POST /api/Record/1": 400})
    ctx = StubContext()
    plan = _plan([{"id": "r", "continue_on_error": True, "aggregate_fallbacks": [],
                   "calls": [_keyed_call("row-1", "/api/Record/1", "k1")]}])
    _run(ctx, plan)
    assert ctx.state.get("completed_call_keys") in (None, [])

    fake.responses.clear()
    _run(ctx, _plan([{"id": "r", "aggregate_fallbacks": [],
                      "calls": [_keyed_call("row-1", "/api/Record/1", "k1")]}], plan_hash="h2"))
    assert len(fake.calls) == 2


def test_a_successful_fallback_marks_the_call_delivered(http):
    fake = http({"PATCH /api/E/e1": 404})
    ctx = StubContext()
    call = _keyed_call("e1", "/api/E/e1", "k1")
    call["method"] = "PATCH"
    call["on_status"] = {"404": {"method": "POST", "path": "/api/E", "headers": {}, "body": {}}}
    _run(ctx, _plan([{"id": "e", "aggregate_fallbacks": [], "calls": [call]}]))
    assert ctx.state["completed_call_keys"] == ["k1"]


def test_dedupe_can_be_disabled_per_step(http):
    fake = http()
    ctx = StubContext()
    for plan_hash in ("h1", "h2"):
        _run(ctx, _plan([{"id": "r", "aggregate_fallbacks": [],
                          "calls": [_keyed_call("x", "/api/x", "k1", dedupe=False)]}],
                        plan_hash=plan_hash))
    assert len(fake.calls) == 2


def test_delivered_call_history_is_bounded(http):
    http()
    ctx = StubContext(
        state={"completed_call_keys": [f"k{i}" for i in range(api_call_plan._COMPLETED_CALLS_HISTORY)]}
    )
    _run(ctx, _plan([{"id": "r", "aggregate_fallbacks": [],
                      "calls": [_keyed_call("x", "/api/x", "brand-new")]}]))
    assert len(ctx.state["completed_call_keys"]) == api_call_plan._COMPLETED_CALLS_HISTORY
    assert ctx.state["completed_call_keys"][-1] == "brand-new"


# --- aggregate fallback ordering & envelope -------------------------------


def test_aggregate_fallback_completes_before_the_next_step_starts(http):
    """Later steps assume the entities the fallback created now exist."""
    fake = http({"PATCH /entity/e2": 404})
    plan = _plan(
        [
            AGGREGATE_STEP,
            {"id": "after", "aggregate_fallbacks": [], "calls": [_call("single", "/after")]},
        ]
    )
    _run(StubContext(), plan)

    paths = [c["path"] for c in fake.calls]
    assert paths.index("/entity") < paths.index("/after")


def test_aggregate_envelope_keeps_its_static_fields(http):
    """collect_into fills a slot in a full payload, not a bare list."""
    fake = http({"PATCH /entity/e1": 404})
    step = {
        **AGGREGATE_STEP,
        "aggregate_fallbacks": [
            {
                "status": 404,
                "method": "POST",
                "path": "/entity",
                "headers": {},
                "body": {
                    "deleteMissingEntities": False,
                    "source": "telemetry",
                    "entities": [],
                },
                "collect_into": "entities",
            }
        ],
    }
    _run(StubContext(), _plan([step]))

    body = [c for c in fake.calls if c["path"] == "/entity"][0]["body"]
    assert body["deleteMissingEntities"] is False
    assert body["source"] == "telemetry"
    assert [e["entityIdentifier"] for e in body["entities"]] == ["e1"]


def test_calls_delivered_via_the_aggregate_fallback_are_recorded_as_delivered(http):
    """The aggregate call delivers on behalf of the calls that failed.

    If those keys are not banked, a later overlapping dispatch re-runs
    them — and the accounting under-reports what actually landed. Found
    by a live Restate run reporting 7 delivered for 8 executed calls.
    """
    http({"PATCH /entity/e2": 404})
    ctx = StubContext()
    step = {
        **AGGREGATE_STEP,
        "calls": [dict(c, call_key=f"k-{c['item_key']}", dedupe=True)
                  for c in AGGREGATE_STEP["calls"]],
    }
    _run(ctx, _plan([step]))

    # e1 and e3 succeeded directly; e2 succeeded via the aggregate create.
    assert sorted(ctx.state["completed_call_keys"]) == ["k-e1", "k-e2", "k-e3"]


def test_a_failed_aggregate_fallback_does_not_mark_its_items_delivered(http):
    http({"PATCH /entity/e2": 404, "POST /entity": 400})
    ctx = StubContext()
    step = {
        **AGGREGATE_STEP,
        "continue_on_error": True,
        "calls": [dict(c, call_key=f"k-{c['item_key']}", dedupe=True)
                  for c in AGGREGATE_STEP["calls"]],
    }
    _run(ctx, _plan([step]))
    assert "k-e2" not in ctx.state["completed_call_keys"]

"""The per-API concurrency cap, the retry bound and the permit-wait guard.

Everything here runs the real handler against a stub context and a fake
HTTP session whose "request" holds a thread for a while, so in-flight counts
are real thread counts. The Restate-side behaviour (timers, aborts) is not
simulated here: that is the opt-in live test, and the measurements in
docs/RECIPE.md.
"""
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from datetime import timedelta
from typing import Any, Dict, List

import pytest
import restate
from restate.discovery import compute_discovery_json
from restate.endpoint import Endpoint

from dag_tools.restate_handlers import api_call_plan
from dag_tools.restate_handlers.api_call_plan import PermitWaitExpired, execute_plan
from dag_tools_tests.test_otel_api_sync_executor import (
    AGGREGATE_STEP,
    FakeHttp,
    StubContext,
    _call,
    _fresh_runtime,  # noqa: F401  (autouse fixture)
    http,  # noqa: F401  (fixture)
    _plan,
    patch_session,
)

BASE = "https://api.test"


def _capped_plan(group, cap, *, base=BASE, **api):
    plan = _plan(
        [{"id": "s", "calls": [_call("single", f"/{group}")], "aggregate_fallbacks": []}],
        group_key=group,
        plan_hash=f"hash-{group}",
    )
    plan["api"].update({"base_url": base, "max_concurrency": cap, **api})
    return plan


def _slow(seconds):
    return lambda method, url, body: time.sleep(seconds)


async def _fan_out(plans):
    return await asyncio.gather(
        *(execute_plan(StubContext(key=p["group_key"]), p) for p in plans)
    )


# --- the cap -----------------------------------------------------------------


def test_cap_bounds_calls_in_flight_across_groups(monkeypatch):
    fake = patch_session(monkeypatch, FakeHttp({}))
    fake.during = _slow(0.02)
    plans = [_capped_plan(f"g{i}", 3) for i in range(50)]

    results = asyncio.run(_fan_out(plans))

    assert all(r["status"] == "COMPLETED" for r in results)
    assert len(fake.calls) == 50
    assert fake.max_in_flight == 3


def test_no_cap_is_unbounded_as_before(monkeypatch):
    fake = patch_session(monkeypatch, FakeHttp({}))
    fake.during = _slow(0.1)
    plans = [_capped_plan(f"g{i}", None) for i in range(12)]

    asyncio.run(_fan_out(plans))

    assert fake.max_in_flight > 3
    assert api_call_plan._semaphores == {}


def test_replay_does_not_wait_for_a_permit(monkeypatch):
    """A journaled step returns without invoking the action, so it cannot park."""
    fake = patch_session(monkeypatch, FakeHttp({}))

    class ReplayCtx(StubContext):
        async def run(self, name, action, **kwargs):
            return {"status": 200, "body": {}}  # journaled: the action is never called

    async def scenario():
        sem = api_call_plan._get_semaphore(BASE, 1)
        await sem.acquire()  # every permit is held
        return await asyncio.wait_for(
            execute_plan(ReplayCtx(key="g"), _capped_plan("g", 1)), timeout=2
        )

    result = asyncio.run(scenario())
    assert result["status"] == "COMPLETED"
    assert fake.calls == []


def test_cap_is_per_base_url(monkeypatch):
    fake = patch_session(monkeypatch, FakeHttp({}))
    fake.during = _slow(0.1)
    plans = [_capped_plan("a", 1, base="https://one.test"), _capped_plan("b", 1, base="https://two.test")]

    asyncio.run(_fan_out(plans))

    assert fake.max_in_flight == 2
    assert set(api_call_plan._semaphores) == {"https://one.test", "https://two.test"}


def _action(**kw):
    return api_call_plan._make_request(
        "POST", f"{BASE}/x", {}, {}, kw.pop("timeout", 5.0), [], BASE, kw.pop("cap", 1)
    )


def test_a_cancelled_action_keeps_its_permit_until_the_request_finishes(monkeypatch):
    """Cancelling the task does not stop the thread, so the permit stays held."""
    import threading

    fake = patch_session(monkeypatch, FakeHttp({}))
    release = threading.Event()
    fake.during = lambda method, url, body: release.wait(10)

    async def scenario():
        first = asyncio.ensure_future(_action()())
        for _ in range(200):
            if fake.in_flight == 1:
                break
            await asyncio.sleep(0.01)
        assert fake.in_flight == 1
        second = asyncio.ensure_future(_action()())
        await asyncio.sleep(0.05)
        first.cancel()  # what the SDK does when an attempt ends
        with pytest.raises(asyncio.CancelledError):
            await first
        await asyncio.sleep(0.2)
        # The abandoned request is still running, so the waiter is still parked.
        assert fake.in_flight == 1
        assert len(fake.calls) == 1
        release.set()
        await asyncio.wait_for(second, timeout=5)
        return len(fake.calls)

    assert asyncio.run(scenario()) == 2
    assert fake.max_in_flight == 1


def test_a_parked_call_cancelled_before_its_permit_sends_nothing(monkeypatch):
    import threading

    fake = patch_session(monkeypatch, FakeHttp({}))
    release = threading.Event()
    fake.during = lambda method, url, body: release.wait(10)

    async def scenario():
        holder = asyncio.ensure_future(_action()())
        for _ in range(200):
            if fake.in_flight == 1:
                break
            await asyncio.sleep(0.01)
        parked = asyncio.ensure_future(_action()())
        await asyncio.sleep(0.05)
        parked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await parked
        release.set()
        await asyncio.wait_for(holder, timeout=5)
        # The permit was neither leaked nor double-released.
        await asyncio.wait_for(_action()(), timeout=5)
        return len(fake.calls)

    assert asyncio.run(scenario()) == 2  # holder and the later call, never the parked one


def test_a_cancel_that_lands_as_the_permit_is_granted_sends_nothing_and_returns_the_permit(monkeypatch):
    """Python 3.11's wait_for swallows a cancel that arrives with the result.

    The SDK cancels the action when an attempt ends. If that lands in the
    tick the permit is granted, the abandoned attempt must not send (the retry
    would send it again) and must hand the permit back.
    """
    fake = patch_session(monkeypatch, FakeHttp({}))

    async def scenario():
        semaphore = api_call_plan._get_semaphore(BASE, 1)
        await semaphore.acquire()  # every permit is held
        task = asyncio.ensure_future(_action()())
        for _ in range(5):
            await asyncio.sleep(0)  # let it park on the semaphore
        semaphore.release()
        task.cancel()  # same tick as the grant
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.1)
        return task.cancelled(), semaphore._value

    cancelled, value = asyncio.run(scenario())
    assert fake.calls == []
    assert cancelled
    assert value == 1


def test_permit_wait_past_the_deadline_raises_retryable_before_sending(monkeypatch):
    import threading

    monkeypatch.setenv("API_CALL_PLAN_INACTIVITY_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("API_CALL_PLAN_ABORT_TIMEOUT_SECONDS", "4.5")
    fake = patch_session(monkeypatch, FakeHttp({}))
    release = threading.Event()
    fake.during = lambda method, url, body: release.wait(10)

    async def scenario():
        holder = asyncio.ensure_future(_action(timeout=0.1)())
        for _ in range(200):
            if fake.in_flight == 1:
                break
            await asyncio.sleep(0.01)
        started = time.monotonic()
        with pytest.raises(PermitWaitExpired) as caught:
            await _action(timeout=0.1)()
        elapsed = time.monotonic() - started
        release.set()
        await holder
        return caught.value, elapsed

    exc, elapsed = asyncio.run(scenario())
    # 1 + 4.5 - 0.1 - max(5, 0.55) = 0.4 s: refused, and quickly.
    assert 0.2 < elapsed < 2.0
    assert len(fake.calls) == 1  # the refused call was never sent
    # Retryable: anything but a TerminalError is retried by Restate.
    assert isinstance(exc, RuntimeError) and not isinstance(exc, restate.TerminalError)


def test_permit_deadline_reads_the_worker_timeout_env(monkeypatch, caplog):
    for name in (api_call_plan._INACTIVITY_ENV, api_call_plan._ABORT_ENV):
        monkeypatch.delenv(name, raising=False)
    # Restate's defaults: 660 s bound, margin max(5, 66) = 66.
    assert api_call_plan._permit_deadline_seconds(BASE, 30) == pytest.approx(564)

    monkeypatch.setenv(api_call_plan._INACTIVITY_ENV, "10")
    monkeypatch.setenv(api_call_plan._ABORT_ENV, "20")
    # 30 s bound, margin 5.
    assert api_call_plan._permit_deadline_seconds(BASE, 5) == pytest.approx(20)

    # A timeout too long for the bound is "no wait", never "no deadline", and warns once.
    with caplog.at_level(logging.WARNING, logger=api_call_plan.logger.name):
        assert api_call_plan._permit_deadline_seconds(BASE, 60) == 0.0
        assert api_call_plan._permit_deadline_seconds(BASE, 60) == 0.0
    assert sum("no permit-wait budget" in r.message for r in caplog.records) == 1


# --- the retry bound ----------------------------------------------------------


class BoundedCtx(StubContext):
    """Raises the SDK's TerminalError for steps whose name contains ``marker``."""

    def __init__(self, marker, **kw):
        super().__init__(**kw)
        self.marker = marker

    async def run(self, name, action, **kwargs):
        if self.marker in name:
            self.step_names.append(name)
            raise restate.TerminalError("retry bound exhausted")
        return await super().run(name, action, **kwargs)


def _assert_halted_and_recorded(ctx, result, phase):
    assert result["status"] == "FAILED" and result["halted"] is True
    assert [f["phase"] for f in result["failures"]] == [phase]
    assert result["failures"][0]["error"] == "retry bound exhausted"
    # Recorded for Dagster's check, and the hash withheld so a corrected
    # re-dispatch is let through.
    assert ctx.state["last_result"]["status"] == "FAILED"
    assert "hash1" not in (ctx.state.get("completed_plan_hashes") or [])


def test_exhausted_retry_bound_halts_and_is_recorded_not_raised_primary(http):
    http()
    ctx = BoundedCtx("-two-", key="group-1")
    c1, c2 = _call("single", "/a"), _call("single", "/b")
    c1["call_key"], c2["call_key"] = "k1", "k2"
    plan = _plan(
        [
            {"id": "one", "calls": [c1], "aggregate_fallbacks": []},
            {"id": "two", "calls": [c2], "aggregate_fallbacks": []},
            {"id": "three", "calls": [_call("single", "/c")], "aggregate_fallbacks": []},
        ]
    )
    result = asyncio.run(execute_plan(ctx, plan))
    _assert_halted_and_recorded(ctx, result, "call")
    assert result["calls_executed"] == 1
    assert ctx.state["completed_call_keys"] == ["k1"]  # delivered before the halt is banked
    assert not any("three" in n for n in ctx.step_names)


def test_exhausted_retry_bound_halts_and_is_recorded_not_raised_item_fallback(http):
    http({"POST /a": 404})
    ctx = BoundedCtx("-fallback-", key="group-1")
    call = _call("single", "/a", on_status={"404": {"method": "POST", "path": "/fb", "body": {}}})
    plan = _plan([{"id": "one", "calls": [call], "aggregate_fallbacks": []}])
    result = asyncio.run(execute_plan(ctx, plan))
    _assert_halted_and_recorded(ctx, result, "fallback")


def test_exhausted_retry_bound_halts_and_is_recorded_not_raised_aggregate_fallback(http):
    http({"PATCH /entity/e1": 404})
    ctx = BoundedCtx("-aggregate-", key="group-1")
    result = asyncio.run(execute_plan(ctx, _plan([AGGREGATE_STEP])))
    _assert_halted_and_recorded(ctx, result, "aggregate_fallback")


def test_unbounded_by_default_passes_no_retry_options(http):
    http()
    ctx = StubContext()
    asyncio.run(execute_plan(ctx, _plan([{"id": "s", "calls": [_call("single", "/a")], "aggregate_fallbacks": []}])))
    assert ctx.run_kwargs == [{}]


def test_retry_bound_is_passed_to_ctx_run_when_set(http):
    http()
    ctx = StubContext()
    plan = _plan([{"id": "s", "calls": [_call("single", "/a")], "aggregate_fallbacks": []}])
    plan["api"].update({"max_attempts": 3, "max_retry_seconds": 90})
    asyncio.run(execute_plan(ctx, plan))
    assert ctx.run_kwargs == [{"max_attempts": 3, "max_retry_duration": timedelta(seconds=90)}]


# --- service-level timeouts ----------------------------------------------------


def _discovery(service, version):
    endpoint = Endpoint()
    endpoint.bind(service)
    return json.loads(compute_discovery_json(endpoint, version, "bidi"))


def test_service_timeouts_unset_leave_discovery_unchanged(monkeypatch):
    monkeypatch.delenv(api_call_plan._INACTIVITY_ENV, raising=False)
    monkeypatch.delenv(api_call_plan._ABORT_ENV, raising=False)
    assert api_call_plan._service_options() == {}

    built = restate.VirtualObject(name="ApiCallPlanService", **api_call_plan._service_options())
    today = restate.VirtualObject(name="ApiCallPlanService")
    # v2 is what SDK 0.14 still negotiates with an older server: it must not raise.
    assert _discovery(built, 2) == _discovery(today, 2)
    assert _discovery(built, 3) == _discovery(today, 3)


def test_service_timeouts_from_env_reach_discovery(monkeypatch):
    monkeypatch.setenv(api_call_plan._INACTIVITY_ENV, "90")
    monkeypatch.setenv(api_call_plan._ABORT_ENV, "3600")
    built = restate.VirtualObject(name="ApiCallPlanService", **api_call_plan._service_options())

    service = _discovery(built, 3)["services"][0]
    assert service["inactivityTimeout"] == 90_000
    assert service["abortTimeout"] == 3_600_000


def test_only_the_set_timeout_is_passed(monkeypatch):
    monkeypatch.delenv(api_call_plan._INACTIVITY_ENV, raising=False)
    monkeypatch.setenv(api_call_plan._ABORT_ENV, "3600")
    assert set(api_call_plan._service_options()) == {"abort_timeout"}


# --- one session per thread; the private CA still applies -----------------------


def test_one_session_per_thread_is_reused(monkeypatch):
    built: List[Any] = []

    def factory(pool_maxsize):
        built.append(pool_maxsize)
        return FakeHttp({})

    monkeypatch.setattr(api_call_plan, "_new_session", factory)
    plans = [_capped_plan(f"g{i}", 2) for i in range(20)]
    asyncio.run(_fan_out(plans))
    # One per executor thread that ran anything, never one per call.
    assert 1 <= len(built) < 20


def test_session_requests_still_honour_REQUESTS_CA_BUNDLE(tls_endpoint):
    """The handler's Session must trust a bundle named only by REQUESTS_CA_BUNDLE.

    AGENTS section 4 rests on this: a Session built with a custom adapter
    could have stopped consulting the environment. Run in a clean
    interpreter, because the variable is read per request but a stray
    ambient value would make the negative case meaningless.
    """
    url, ca_path, _ = tls_endpoint
    code = (
        "import sys\n"
        "from dag_tools.restate_handlers.api_call_plan import _new_session\n"
        "try:\n"
        "    _new_session(5).request('GET', sys.argv[1], timeout=15)\n"
        "    print('TRUSTED')\n"
        "except Exception as exc:\n"
        "    print('REJECTED:' + type(exc).__name__)\n"
    )

    def run(**trust):
        env = {
            k: v
            for k, v in os.environ.items()
            if k not in ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE")
        }
        env.update({k: str(v) for k, v in trust.items()})
        out = subprocess.run(
            [sys.executable, "-c", code, url], capture_output=True, text=True, env=env
        )
        return out.stdout.strip() or f"ERROR:{out.stderr.strip()[:200]}"

    assert run().startswith("REJECTED")  # guards the guard: not trusted by default
    assert run(REQUESTS_CA_BUNDLE=ca_path) == "TRUSTED"


from dag_tools_tests.test_chart_ca_certs import tls_endpoint  # noqa: E402,F401

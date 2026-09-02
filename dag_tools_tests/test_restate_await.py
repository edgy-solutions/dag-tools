"""Restate invocations are awaited, not fired and forgotten.

Every endpoint in the field was configured with Restate's ``/send``
suffix, which is the ONE-WAY form: the POST returns 202 as soon as
Restate has durably accepted the message, carrying no indication of
whether the handler subsequently succeeded. The Dagster step went green
either way.

That is wrong for all three of these calls and worst for the request that
starts a cycle. If it fails silently the source is never asked for
anything, and we wait for a completion marker that will never arrive --
with a green run saying it all worked. It matters again for the
completion row: where the control table gates the source's own loading, a
row we believe we wrote and did not leaves them blocked.

Dropping the suffix makes the same HTTP call request/response. The
transport was never the limitation.
"""
import pytest

pytest.importorskip("dagster_dlt")

import httpx
from dagster import DagsterInstance, materialize

from dag_tools.components.restate_dlt_sync.component import (
    SEND_SUFFIX,
    await_endpoint,
)

import dag_tools_tests.test_pdm_component_build as build


# ---------------------------------------------------------------------------
# Endpoint normalisation
# ---------------------------------------------------------------------------


def test_the_send_suffix_is_stripped():
    assert await_endpoint("http://r:8080/Svc/handler/send") == (
        "http://r:8080/Svc/handler"
    )


def test_an_already_awaited_endpoint_is_untouched():
    assert await_endpoint("http://r:8080/Svc/handler") == "http://r:8080/Svc/handler"


def test_a_trailing_slash_does_not_defeat_it():
    assert await_endpoint("http://r:8080/Svc/handler/send/") == (
        "http://r:8080/Svc/handler"
    )


def test_a_handler_named_like_the_suffix_survives():
    """`/sender` merely ends with the same letters. Stripping by string
    rather than by path segment would silently retarget the call."""
    assert await_endpoint("http://r:8080/Svc/sender") == "http://r:8080/Svc/sender"


def test_the_suffix_constant_is_what_restate_uses():
    assert SEND_SUFFIX == "/send"


# ---------------------------------------------------------------------------
# A failing handler must fail the step
# ---------------------------------------------------------------------------


class _Restate:
    """Stand-in ingress. Records the URL actually called, and can answer
    the way Restate does when a handler raises."""

    def __init__(self, fail=False):
        self.urls = []
        self.fail = fail

    def __call__(self, url, json=None, timeout=None, **kw):
        self.urls.append(str(url))
        request = httpx.Request("POST", url)
        if self.fail:
            # Restate surfaces a handler error as a non-2xx on the
            # request/response form. With /send it would have been 202.
            return httpx.Response(500, json={"message": "handler failed"},
                                  request=request)
        return httpx.Response(200, json={"ok": True}, request=request)


def _asset(name, **pipeline):
    defs = build._component(pipeline=pipeline).build_defs(None)
    from dagster import AssetKey

    return next(
        a for a in defs.assets if AssetKey([name]) in getattr(a, "keys", [])
    )


def _materialize(asset, restate):
    import dag_tools.components.restate_dlt_sync.component as mod

    original = mod.httpx.post
    mod.httpx.post = restate
    try:
        return materialize(
            [asset], instance=DagsterInstance.ephemeral(), raise_on_error=False,
        )
    finally:
        mod.httpx.post = original


@pytest.mark.parametrize("name", ["pdm_mei_request", "pdm_load_complete"])
def test_a_successful_handler_lets_the_step_pass(name):
    restate = _Restate()
    assert _materialize(_asset(name), restate).success
    assert restate.urls, "no call was made"
    assert not restate.urls[0].endswith(SEND_SUFFIX), restate.urls


@pytest.mark.parametrize("name", ["pdm_mei_request", "pdm_load_complete"])
def test_a_failing_handler_fails_the_step(name):
    """The whole point. Under /send this returned 202 and the step went
    green with nothing written to the source."""
    restate = _Restate(fail=True)
    assert not _materialize(_asset(name), restate).success


def test_the_request_that_starts_a_cycle_cannot_fail_silently():
    """Called out separately because its silent failure is the worst
    shape in the flow: nothing is asked for, so no completion marker ever
    arrives, and the cycle waits forever behind a green run."""
    assert not _materialize(_asset("pdm_mei_request"), _Restate(fail=True)).success

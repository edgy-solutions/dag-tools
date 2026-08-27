"""The gate's authz subject follows USER_ENTITLEMENT_CLAIM, like everything else.

WHY THIS EXISTS. `check_topaz_authz` hardcoded `claims.get("email")` while
`subject_gauge` — sited on the same request, on the same two inputs — read
`USER_ENTITLEMENT_CLAIM`. Dormant, because both said "email" by default.

Set the env var to `preferred_username` (which work-deploy does) and the two
diverge in the worst available direction: the GAUGE resolves a subject and
reports a healthy, agreeing request, while the GATE looks for an `email` claim
the token does not carry and fail-closed denies every read. An instrument
reporting green through a total outage is worse than no instrument, so the
divergence is what these tests refuse.

Note the asymmetry they encode: the HEADER path is claim-agnostic — whatever
`X-Originator-Email` carries is used verbatim, which is why Engine DA's reads
work today regardless of how the claim is configured. Only the TOKEN path
reads a claim, and the token path is the one a notebook standing on its own
identity depends on.
"""
from __future__ import annotations

import asyncio

import jwt
import pytest

# central_gateway.main imports redis and PyJWT at MODULE level; both live in the
# `broker` extra. Skipping keeps the suite collectable without it — but a skip
# is not a pass, so run with `pip install "edgy-dag-tools[broker]"` if you need
# this gate covered. The first test module to import the gateway at all.
pytest.importorskip("redis", reason="gateway tests need the [broker] extra")

from dag_tools.central_gateway import main as gateway  # noqa: E402
from dag_tools.central_gateway import subject_gauge  # noqa: E402


def _token(**claims) -> str:
    return jwt.encode({"sub": "abc-123", **claims}, "k", algorithm="HS256")


@pytest.fixture
def topaz_allow(monkeypatch):
    """Capture the subject the gate sends to Topaz, and always allow."""
    seen = {}

    class _Resp:
        status_code = 200

        @staticmethod
        def json():
            return {"decisions": [{"decision": "allowed", "is": True}]}

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None, timeout=None):
            seen["subject"] = (json or {}).get("resource_context", {}).get("user_id")
            return _Resp()

    monkeypatch.setattr(gateway.httpx, "AsyncClient", lambda *a, **k: _Client())
    return seen


def test_token_claim_follows_the_env(monkeypatch, topaz_allow):
    """preferred_username realm: the gate must read preferred_username."""
    monkeypatch.setenv("USER_ENTITLEMENT_CLAIM", "preferred_username")
    token = _token(preferred_username="e21138247")

    asyncio.run(gateway.check_topaz_authz(token, "urn:li:dataset:(x,y,PROD)"))

    assert topaz_allow["subject"] == "e21138247", (
        "the gate read a different claim than USER_ENTITLEMENT_CLAIM names, so "
        "it authorizes a subject nobody configured"
    )


def test_gate_and_gauge_never_disagree(monkeypatch):
    """Structural: both resolve the subject through one helper."""
    monkeypatch.setenv("USER_ENTITLEMENT_CLAIM", "preferred_username")
    token = _token(preferred_username="alice", email="alice@example.com")

    claims = jwt.decode(token, options={"verify_signature": False})
    gauge_subject = subject_gauge.verify_bearer(token).authz_id

    assert gauge_subject == claims[subject_gauge.entitlement_claim()] == "alice", (
        "gauge and gate resolved different subjects — the gauge would report a "
        "healthy request while the gate denies it"
    )


def test_email_remains_the_default(monkeypatch, topaz_allow):
    """No env var set: unchanged behaviour, so this is not a breaking change."""
    monkeypatch.delenv("USER_ENTITLEMENT_CLAIM", raising=False)
    token = _token(email="bob@example.com")

    asyncio.run(gateway.check_topaz_authz(token, "urn:li:dataset:(x,y,PROD)"))

    assert topaz_allow["subject"] == "bob@example.com"


def test_header_still_wins_and_is_claim_agnostic(monkeypatch, topaz_allow):
    """DA's path is untouched: the header is used verbatim, whatever the claim.

    This is why the one-liner is safe to deploy independently of
    [[da-sends-no-user-token]] — it ADDS a working token path without altering
    the header path DA rides on.
    """
    monkeypatch.setenv("USER_ENTITLEMENT_CLAIM", "preferred_username")
    token = _token(preferred_username="from-token")

    asyncio.run(gateway.check_topaz_authz(
        token, "urn:li:dataset:(x,y,PROD)", originator_email="from-header",
    ))

    assert topaz_allow["subject"] == "from-header"


def test_missing_claim_still_fails_closed(monkeypatch, topaz_allow):
    """A token without the configured claim denies — it does not fall back."""
    monkeypatch.setenv("USER_ENTITLEMENT_CLAIM", "preferred_username")
    token = _token(email="alice@example.com")  # has email, NOT preferred_username

    ok, cols, filters = asyncio.run(
        gateway.check_topaz_authz(token, "urn:li:dataset:(x,y,PROD)")
    )

    assert ok is False, (
        "silently falling back to another claim would authorize a subject the "
        "deployment did not configure"
    )

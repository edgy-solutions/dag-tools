"""The authz subject follows USER_ENTITLEMENT_CLAIM, like everything else.

WHY THIS EXISTS. `check_topaz_authz` hardcoded `claims.get("email")` while
`subject_gauge` — sited on the same request, on the same two inputs — read
`USER_ENTITLEMENT_CLAIM`. Dormant, because both said "email" by default.

Set the env var to `preferred_username` (which work-deploy does) and the two
diverge in the worst available direction: the GAUGE resolves a subject and
reports a healthy, agreeing request, while the GATE looks for an `email` claim
the token does not carry and fail-closed denies every read. An instrument
reporting green through a total outage is worse than no instrument, so the
divergence is what these tests refuse.

WHAT MOVED. The gate no longer resolves anything: `identity.resolve_principal`
is the one resolution, and `check_topaz_authz` is handed its answer. So these
tests drive the claim through that one resolution and keep asserting the
subject that reaches TOPAZ — the property is unchanged and is now pinned where
the reading actually happens. Two further things follow from the move and are
pinned here too: the token path requires a VERIFIED token (an unverified claim
names nobody), and a token missing the configured claim no longer falls back to
another claim NOR to nobody-and-serve — it is refused.

Note the asymmetry they encode: the HEADER path is claim-agnostic — whatever
`X-Originator-Email` carries is used verbatim, which is why Engine DA's reads
work today regardless of how the claim is configured. Only the TOKEN path
reads a claim, and the token path is the one a notebook standing on its own
identity depends on.
"""
from __future__ import annotations

import asyncio
import time

import jwt
import pytest

# central_gateway.main imports redis and PyJWT at MODULE level; both live in the
# `broker` extra. Skipping keeps the suite collectable without it — but a skip
# is not a pass, so run with `pip install "edgy-dag-tools[broker]"` if you need
# this gate covered. The first test module to import the gateway at all.
pytest.importorskip("redis", reason="gateway tests need the [broker] extra")

from dag_tools.central_gateway import identity  # noqa: E402
from dag_tools.central_gateway import main as gateway  # noqa: E402
from dag_tools.central_gateway import subject_gauge  # noqa: E402

ISS = "https://idp.invalid/realms/r"
AUD = "data-gateway"
KEY = "k" * 32


def _token(*, key=KEY, iss=ISS, aud=AUD, **claims) -> str:
    # `exp` is in the verifier's `require` list, so a token without one is invalid rather than
    # eternal — which is the right way round, and worth not tripping over in a fixture.
    return jwt.encode(
        {"sub": "abc-123", "iss": iss, "aud": aud, "exp": int(time.time()) + 300, **claims},
        key, algorithm="HS256",
    )


@pytest.fixture(autouse=True)
def verifying(monkeypatch):
    """Serve the gateway an issuer list, an audience list and a key, so a token can VERIFY.

    Required now rather than incidental: the claim is read only off a token this gateway
    verified, so a suite that set no verification values would be asserting the claim-follows-env
    property over a path that refuses every token — green, and about nothing.
    """
    monkeypatch.setenv("GATEWAY_JWT_ISSUERS", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCES", AUD)
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", KEY)
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "HS256")
    monkeypatch.delenv("GATEWAY_IDENTITY_POSTURE", raising=False)


def _principal(token, header=None):
    """The one resolution, as the route calls it."""
    return identity.resolve_principal(
        token, header, None, identity.ROUTE_AUTHORIZE, "urn:li:dataset:(x,y,PROD)"
    )


def _gate(token, header=None):
    """Drive the real gate over the real resolution, as the route does."""
    return asyncio.run(
        gateway.check_topaz_authz(
            token, "urn:li:dataset:(x,y,PROD)", _principal(token, header)
        )
    )


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
    """preferred_username realm: the resolution must read preferred_username."""
    monkeypatch.setenv("USER_ENTITLEMENT_CLAIM", "preferred_username")
    token = _token(preferred_username="e21138247")

    _gate(token)

    assert topaz_allow["subject"] == "e21138247", (
        "the resolution read a different claim than USER_ENTITLEMENT_CLAIM names, "
        "so it authorizes a subject nobody configured"
    )


def test_gate_and_gauge_read_one_claim_name(monkeypatch):
    """Structural: the resolution and the gauge read one env var for the claim's NAME.

    They deliberately differ on whether the token had to verify (see `subject_gauge.classify`):
    the gauge reads the wider population so it can warn about the narrower one. The claim NAME
    is the part that must never diverge, because a divergence there denies every read while the
    gauge reports green.
    """
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

    _gate(token)

    assert topaz_allow["subject"] == "bob@example.com"


def test_header_still_wins_and_is_claim_agnostic(monkeypatch, topaz_allow):
    """DA's path is untouched: the header is used verbatim, whatever the claim.

    This is why the one-liner is safe to deploy independently of
    [[da-sends-no-user-token]] — it ADDS a working token path without altering
    the header path DA rides on.
    """
    monkeypatch.setenv("USER_ENTITLEMENT_CLAIM", "preferred_username")
    token = _token(preferred_username="from-token")

    _gate(token, header="from-header")

    assert topaz_allow["subject"] == "from-header"


def test_the_header_wins_even_over_a_token_that_did_NOT_verify(monkeypatch, topaz_allow):
    """The claim-agnostic header path does not depend on the token at all.

    Engine DA presents a service-account token and names the user in the header. Tightening the
    token fallback must not have tightened this: the header is the path every reader rides.
    """
    monkeypatch.setenv("USER_ENTITLEMENT_CLAIM", "preferred_username")
    token = _token(preferred_username="from-token", key="x" * 32)  # signed by the wrong key

    _gate(token, header="from-header")

    assert topaz_allow["subject"] == "from-header"


def test_an_UNVERIFIED_tokens_claim_names_nobody(monkeypatch, topaz_allow):
    """The claim is read only off a token this gateway verified.

    The gate used to fall back to an unverified claim, so a request was authorized for the
    subject it CLAIMED to be. A claim nothing has checked is the request's own say-so.
    """
    monkeypatch.setenv("USER_ENTITLEMENT_CLAIM", "preferred_username")
    token = _token(preferred_username="e21138247", key="x" * 32)

    decision = _principal(token)

    assert decision.subject is None
    assert decision.refused == identity.NO_PRINCIPAL
    assert topaz_allow == {}, "an unverified claim must never reach the authorizer"


def test_missing_claim_still_fails_closed(monkeypatch, topaz_allow):
    """A token without the configured claim denies — it does not fall back."""
    monkeypatch.setenv("USER_ENTITLEMENT_CLAIM", "preferred_username")
    token = _token(email="alice@example.com")  # has email, NOT preferred_username

    decision = _principal(token)
    assert decision.subject is None, (
        "silently falling back to another claim would authorize a subject the "
        "deployment did not configure"
    )
    assert decision.refused == identity.NO_PRINCIPAL

    ok, cols, filters = asyncio.run(
        gateway.check_topaz_authz(token, "urn:li:dataset:(x,y,PROD)", decision)
    )
    assert ok is False, "a gate handed a principal of None must deny, not ask the authorizer"
    assert topaz_allow == {}

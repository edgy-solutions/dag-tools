"""The gateway decides who a request reads as from a VERIFIED token (`enforce`), and under
`observe` (the default) changes nothing.

THE RULING these pin:
  1. A user token's identity is its verified claim; a header that differs is refused.
  2. A verified service token may name a user only if the policy rail's delegate seam lists that service for
     the route; the audit names both.
  3. The header is `on_behalf_of` on the wire. A blank one from a delegate means the service acts
     as itself.

Every test drives the REAL route with a real RSA keypair, fakes only Redis, Topaz and the broker,
and asserts the SUBJECT the Topaz call received, not just the status. A refusal asserts that
Topaz was never called at all.
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

jwt = pytest.importorskip("jwt")
pytest.importorskip("redis", reason="gateway tests need the [broker] extra")
pytest.importorskip("cryptography", reason="RS256 tests need cryptography")

from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from dag_tools.central_gateway import identity  # noqa: E402
from dag_tools.central_gateway import main as gateway  # noqa: E402

_DEFAULT_SEAM = identity.is_delegate
ISS = "https://idp.example/realms/mesh"
IN_CLUSTER_ISS = "http://idp.idp.svc.cluster.local/realms/mesh"
AUD = "data-gateway"
URN = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/x,PROD)"
AUTHZ = f"/api/v1/assets/{URN}/authorize"
ROUTE = identity.ROUTE_AUTHORIZE


def _keypair():
    k = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    priv = k.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    pub = k.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode()
    return priv, pub


PRIV, PUB = _keypair()
OTHER_PRIV, _OTHER_PUB = _keypair()


def _tok(claims=None, *, key=PRIV, iss=ISS, aud=AUD, exp=300, alg="RS256", **extra):
    body = {"sub": "kc-1", "iss": iss, "aud": aud, "exp": int(time.time()) + exp,
            **(claims or {}), **extra}
    return jwt.encode(body, key, algorithm=alg)


def user_tok(email="alice@example.com", **kw):
    return _tok({"email": email}, **kw)


def svc_tok(name="svc:etl", **kw):
    return _tok({"email": name, "sub": "kc-svc"}, **kw)


# ---------------------------------------------------------------------------
# Fixtures: the real route, fake Redis / Topaz / broker
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for k in (
        "GATEWAY_IDENTITY_POSTURE", "GATEWAY_JWT_ISSUER", "GATEWAY_JWT_ISSUERS",
        "GATEWAY_JWT_AUDIENCE", "GATEWAY_JWT_AUDIENCES", "GATEWAY_JWKS_URL",
        "GATEWAY_JWT_PUBLIC_KEY", "KEYCLOAK_PUBLIC_KEY", "GATEWAY_JWT_ALGORITHMS",
        "USER_ENTITLEMENT_CLAIM",
    ):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(identity, "_unconfigured_said", None)
    monkeypatch.setattr(identity, "_jwks_client", None)
    monkeypatch.setattr(identity, "_jwks_client_url", None)
    monkeypatch.setattr(gateway, "DENIED_USER_SUBS", set())


@pytest.fixture
def world(monkeypatch):
    """Returns an object with `.topaz` (list of subjects Topaz was asked about) and `.broker`
    (the payloads sent to the broker)."""
    w = SimpleNamespace(topaz=[], broker=[])

    class _Redis:
        async def get(self, key):
            return "http://broker.invalid"

    class _Resp:
        def __init__(self, code, body):
            self.status_code, self._b, self.text = code, body, ""

        def json(self):
            return self._b

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None, timeout=None):
            if "/authz/is" in url:
                w.topaz.append((json or {}).get("resource_context", {}).get("user_id"))
                return _Resp(200, {"decisions": [{"is": True}]})
            w.broker.append(json)
            return _Resp(200, {"source_type": "s3_parquet", "physical_uri": "s3://b/x"})

    monkeypatch.setattr(gateway, "redis_client", _Redis())
    monkeypatch.setattr(gateway.httpx, "AsyncClient", lambda *a, **k: _Client())
    return w


@pytest.fixture
def http():
    return TestClient(gateway.app)


def call(http, token, **headers):
    h = {"Authorization": f"Bearer {token}"}
    h.update({k.replace("_", "-") if k.startswith("X") else k: v for k, v in headers.items()})
    return http.post(AUTHZ, headers=h)


@pytest.fixture
def enforce(monkeypatch):
    monkeypatch.setenv("GATEWAY_IDENTITY_POSTURE", "enforce")
    monkeypatch.setenv("GATEWAY_JWT_ISSUER", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCE", AUD)
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", PUB)
    rail(monkeypatch, {("svc:etl", ROUTE)})


def rail(monkeypatch, listed):
    """Install a FAKE delegate seam. `listed` is a set of (service, route); the callable's calls
    are recorded on `.calls`. Pass "raise" or "unknown" for the failure arms."""
    calls = []

    def seam(service, route):
        calls.append((service, route))
        if listed == "raise":
            raise RuntimeError("rail unreadable")
        if listed == "unknown":
            return None
        return (service, route) in listed

    seam.calls = calls
    monkeypatch.setattr(identity, "is_delegate", seam)
    return seam


@pytest.fixture
def lines(caplog):
    caplog.set_level(logging.INFO, logger="dag_tools.central_gateway")
    caplog.set_level(logging.INFO, logger="dag_tools.central_gateway.identity")

    def grep(token):
        return [r.getMessage() for r in caplog.records if token in r.getMessage()]
    return grep


def _reason(resp):
    d = resp.json()["detail"]
    return d["error"] if isinstance(d, dict) else d


# ---------------------------------------------------------------------------
# ENFORCE — refusals: each names its reason and never reaches Topaz
# ---------------------------------------------------------------------------
def _refusal_cases():
    return {
        "forged_signature": (lambda: user_tok(key=OTHER_PRIV), {}, {}, 401, "token_invalid"),
        "expired": (lambda: user_tok(exp=-60), {}, {}, 401, "token_invalid"),
        "wrong_issuer": (lambda: user_tok(iss="https://evil.example/realms/mesh"), {}, {}, 401, "issuer_not_accepted"),
        "wrong_audience": (lambda: user_tok(aud="some-other-client"), {}, {}, 401, "token_invalid"),
        "no_audience_claim": (lambda: user_tok(aud=None), {}, {}, 401, "token_invalid"),
        "no_audience_configured": (lambda: user_tok(), {}, {"GATEWAY_JWT_AUDIENCE": ""}, 503, "verification_unconfigured"),
        "alg_none": (lambda: jwt.encode({"sub": "k", "iss": ISS, "exp": int(time.time()) + 300,
                                         "email": "alice@example.com"}, None, algorithm="none"),
                     {}, {}, 401, "token_invalid"),
        "garbage": (lambda: "not.a.jwt", {}, {}, 401, "token_invalid"),
        "unlisted_service_naming_a_user": (lambda: svc_tok("svc:other"), {"on_behalf_of": "alice@example.com"}, {}, 403, "not_a_delegate"),
        "listed_service_other_route": (lambda: svc_tok(), {"on_behalf_of": "alice@example.com"},
                                       {"RAIL": {("svc:etl", "assets.resolve")}}, 403, "not_a_delegate"),
        "user_with_differing_header": (lambda: user_tok(), {"on_behalf_of": "bob@example.com"}, {}, 403, "on_behalf_of_differs"),
        "user_with_differing_legacy_header": (lambda: user_tok(), {"X-Originator-Email": "bob@example.com"}, {}, 403, "on_behalf_of_differs"),
        "legacy_header_alone_from_listed_service": (lambda: svc_tok(), {"X-Originator-Email": "alice@example.com"}, {}, 403, "legacy_header"),
        "both_headers_differing": (lambda: svc_tok(), {"on_behalf_of": "alice@example.com",
                                                       "X-Originator-Email": "bob@example.com"}, {}, 403, "on_behalf_of_differs"),
        "no_issuer_configured": (lambda: user_tok(), {}, {"GATEWAY_JWT_ISSUER": ""}, 503, "verification_unconfigured"),
        # An issuer this gateway was not told to accept is refused for THAT reason -- including
        # the in-cluster name of the very IdP it does accept, which is why that name has to be
        # listed as a second entry rather than assumed.
        "in_cluster_issuer_not_listed": (lambda: user_tok(iss=IN_CLUSTER_ISS), {}, {}, 401, "issuer_not_accepted"),
        "no_key_source": (lambda: user_tok(), {}, {"GATEWAY_JWT_PUBLIC_KEY": ""}, 503, "verification_unconfigured"),
        "no_subject_claim": (lambda: _tok({}), {}, {}, 403, "no_subject"),
        "seam_raises_refuses_as_unlisted": (lambda: svc_tok(), {"on_behalf_of": "alice@example.com"},
                                            {"RAIL": "raise"}, 403, "not_a_delegate"),
        "seam_unknown_refuses_as_unlisted": (lambda: svc_tok(), {"on_behalf_of": "alice@example.com"},
                                             {"RAIL": "unknown"}, 403, "not_a_delegate"),
    }


@pytest.mark.parametrize("case", sorted(_refusal_cases()))
def test_enforce_refuses_and_never_calls_topaz(case, enforce, world, http, monkeypatch, lines):
    mk, headers, env, status, reason = _refusal_cases()[case]
    for k, v in env.items():
        if k == "RAIL":
            rail(monkeypatch, v)
        else:
            monkeypatch.setenv(k, v)
    r = call(http, mk(), **headers)
    assert r.status_code == status, r.text
    assert _reason(r) == reason
    assert world.topaz == [], "Topaz was consulted for a request the identity gate refused"
    assert world.broker == []
    audit = lines("IDENTITY_AUDIT")
    assert audit and "outcome=refuse" in audit[-1] and f"reason={reason}" in audit[-1]


def test_enforce_refusal_never_logs_a_token(enforce, world, http, lines):
    tok = user_tok(key=OTHER_PRIV)
    call(http, tok)
    assert not [m for m in lines("") if tok in m]


def test_enforce_absent_token_is_token_absent(enforce, world, http):
    # HTTPBearer itself refuses a missing header; a blank bearer reaches decide.
    d = identity.decide("", None, None, ROUTE)
    assert (d.refused, d.status) == ("token_absent", 401)


def test_jwks_unreachable_refuses_then_recovers_without_latching(enforce, world, http, monkeypatch, lines):
    monkeypatch.delenv("GATEWAY_JWT_PUBLIC_KEY")
    monkeypatch.setenv("GATEWAY_JWKS_URL", "https://jwks.invalid/keys")
    state = {"up": False, "made": 0}

    class _Jwks:
        def get_signing_key_from_jwt(self, token):
            if not state["up"]:
                raise jwt.PyJWKClientConnectionError("down")
            return SimpleNamespace(key=PUB)

    def _make(url):
        state["made"] += 1
        return _Jwks()

    monkeypatch.setattr(identity, "_make_jwks_client", _make)
    tok = user_tok()

    r1 = call(http, tok)
    assert (r1.status_code, _reason(r1)) == (503, "jwks_unavailable")
    assert world.topaz == []

    state["up"] = True
    r2 = call(http, tok)
    assert r2.status_code == 200, r2.text
    assert world.topaz == ["alice@example.com"]


def test_a_token_whose_kid_is_not_in_the_jwks_is_invalid_not_unavailable(enforce, monkeypatch):
    monkeypatch.delenv("GATEWAY_JWT_PUBLIC_KEY")
    monkeypatch.setenv("GATEWAY_JWKS_URL", "https://jwks.invalid/keys")

    class _Jwks:
        def get_signing_key_from_jwt(self, token):
            raise jwt.PyJWKClientError("Unable to find a signing key that matches")

    monkeypatch.setattr(identity, "_make_jwks_client", lambda url: _Jwks())
    d = identity.decide(user_tok(), None, None, ROUTE)
    assert (d.refused, d.status) == ("token_invalid", 401)


# ---------------------------------------------------------------------------
# ENFORCE — allowed: assert the SUBJECT Topaz received and what the audit names
# ---------------------------------------------------------------------------
def test_enforce_user_own_token_no_header(enforce, world, http, lines):
    r = call(http, user_tok("alice@example.com"))
    assert r.status_code == 200
    assert world.topaz == ["alice@example.com"]
    assert world.broker[0]["subject"] == "alice@example.com"
    assert "subject=alice@example.com via=- outcome=allow" in lines("IDENTITY_AUDIT")[-1]


@pytest.mark.parametrize("hdr", [{"on_behalf_of": "alice@example.com"},
                                 {"X-Originator-Email": "alice@example.com"},
                                 {"on_behalf_of": " alice@example.com ",
                                  "X-Originator-Email": "alice@example.com"}])
def test_enforce_user_own_token_equal_header(hdr, enforce, world, http):
    r = call(http, user_tok("alice@example.com"), **hdr)
    assert r.status_code == 200
    assert world.topaz == ["alice@example.com"]


def test_enforce_listed_service_names_a_user(enforce, world, http, lines):
    r = call(http, svc_tok(), on_behalf_of="alice@example.com")
    assert r.status_code == 200
    assert world.topaz == ["alice@example.com"], "Topaz must see the user, not the service"
    assert world.broker[0]["subject"] == "alice@example.com"
    line = lines("IDENTITY_AUDIT")[-1]
    assert "subject=alice@example.com" in line and "via=svc:etl" in line
    assert f"route={ROUTE}" in line and "outcome=allow" in line


@pytest.mark.parametrize("blank", [{}, {"on_behalf_of": "   "}])
def test_enforce_listed_service_blank_acts_as_itself(blank, enforce, world, http, lines):
    r = call(http, svc_tok(), **blank)
    assert r.status_code == 200
    assert world.topaz == ["svc:etl"]
    assert "subject=svc:etl" in lines("IDENTITY_AUDIT")[-1]


def test_enforce_honours_the_configured_claim(enforce, world, http, monkeypatch):
    monkeypatch.setenv("USER_ENTITLEMENT_CLAIM", "preferred_username")
    r = call(http, _tok({"preferred_username": "e21138247"}))
    assert r.status_code == 200
    assert world.topaz == ["e21138247"]


def test_enforce_does_not_read_a_claim_from_an_unverified_decode(enforce, world, http, monkeypatch):
    """The Topaz subject comes from the decision. A token naming no entitlement claim is refused
    `no_subject`; it never falls back to some other unverified claim."""
    r = call(http, _tok({"preferred_username": "mallory"}))
    assert r.status_code == 403 and _reason(r) == "no_subject"
    assert world.topaz == []


# ---------------------------------------------------------------------------
# ENFORCE — the deny list uses the VERIFIED sub only
# ---------------------------------------------------------------------------
def test_enforce_deny_list_uses_the_verified_sub_not_the_originator_sub(enforce, world, http, monkeypatch):
    monkeypatch.setattr(gateway, "DENIED_USER_SUBS", {"kc-bad"})
    # The header names a denied sub; the verified token is someone else. A forged header must
    # not be able to choose who the list is applied to: the request is allowed.
    r = call(http, user_tok("alice@example.com"), **{"X-Originator-Sub": "kc-bad"})
    assert r.status_code == 200, "X-Originator-Sub influenced the deny list under enforce"
    # And the verified sub IS the one applied.
    r2 = call(http, user_tok("alice@example.com", sub="kc-bad"), **{"X-Originator-Sub": "kc-fine"})
    assert r2.status_code == 403
    assert "deny list" in r2.json()["detail"]


def test_deny_1_listed_delegate_originator_sub_in_list_is_refused(enforce, world, http, monkeypatch):
    monkeypatch.setattr(gateway, "DENIED_USER_SUBS", {"kc-bad"})
    r = call(http, svc_tok(), on_behalf_of="alice@example.com", **{"X-Originator-Sub": "kc-bad"})
    assert r.status_code == 403 and "deny list" in r.json()["detail"]
    assert world.topaz == []


def test_deny_2_listed_delegate_sub_not_in_list_is_allowed(enforce, world, http, monkeypatch):
    monkeypatch.setattr(gateway, "DENIED_USER_SUBS", {"kc-bad"})
    r = call(http, svc_tok(), on_behalf_of="alice@example.com", **{"X-Originator-Sub": "kc-fine"})
    assert r.status_code == 200
    assert world.topaz == ["alice@example.com"]


def test_deny_3_user_token_own_sub_denied_is_refused_despite_other_header(enforce, world, http, monkeypatch):
    monkeypatch.setattr(gateway, "DENIED_USER_SUBS", {"kc-bad"})
    r = call(http, user_tok("alice@example.com", sub="kc-bad"), **{"X-Originator-Sub": "kc-fine"})
    assert r.status_code == 403
    assert world.topaz == []


def test_deny_4_user_token_not_denied_header_names_denied_sub_is_allowed(enforce, world, http, monkeypatch):
    monkeypatch.setattr(gateway, "DENIED_USER_SUBS", {"kc-bad"})
    r = call(http, user_tok("alice@example.com"), **{"X-Originator-Sub": "kc-bad"})
    assert r.status_code == 200
    assert world.topaz == ["alice@example.com"]


# ---------------------------------------------------------------------------
# The posture parse
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("raw,want", [(None, "observe"), ("", "observe"), ("  ", "observe"),
                                      ("observe", "observe"), ("OBSERVE", "observe"),
                                      ("enforce", "enforce"), ("Enforce", "enforce"),
                                      ("enforced", "enforce"), ("off", "enforce")])
def test_posture_parse(raw, want, monkeypatch):
    if raw is None:
        monkeypatch.delenv("GATEWAY_IDENTITY_POSTURE", raising=False)
    else:
        monkeypatch.setenv("GATEWAY_IDENTITY_POSTURE", raw)
    assert identity.posture() == want


def test_an_unknown_posture_is_enforce_and_logged_at_error(monkeypatch, caplog):
    monkeypatch.setenv("GATEWAY_IDENTITY_POSTURE", "advisory")
    with caplog.at_level(logging.ERROR):
        assert identity.posture() == "enforce"
    assert [r for r in caplog.records if r.levelno == logging.ERROR]


def test_the_default_seam_is_false_and_says_once_that_the_rail_is_not_wired(monkeypatch, caplog):
    monkeypatch.setattr(identity, "_rail_warned", False)
    with caplog.at_level(logging.ERROR):
        assert identity.is_delegate("svc:etl", ROUTE) is False
        assert identity.is_delegate("svc:etl", ROUTE) is False
    errs = [r for r in caplog.records if r.levelno == logging.ERROR and "not wired" in r.getMessage()]
    assert len(errs) == 1


def test_with_the_default_seam_a_service_naming_a_user_is_refused(enforce, monkeypatch):
    monkeypatch.setattr(identity, "is_delegate", _DEFAULT_SEAM)
    d = identity.decide(svc_tok(), "alice@example.com", None, ROUTE)
    assert d.refused == "not_a_delegate"


def test_the_seam_is_called_per_request_with_the_service_and_the_ROUTE(enforce, world, http, monkeypatch):
    seam = rail(monkeypatch, {("svc:etl", ROUTE)})
    call(http, svc_tok(), on_behalf_of="alice@example.com")
    call(http, svc_tok(), on_behalf_of="bob@example.com")
    assert seam.calls == [("svc:etl", ROUTE), ("svc:etl", ROUTE)]


def test_an_injected_seam_argument_wins_over_the_module_seam(enforce):
    d = identity.decide(svc_tok(), "alice@example.com", None, ROUTE, is_delegate_fn=lambda s, r: False)
    assert d.refused == "not_a_delegate"


def test_settings_are_read_per_call_not_at_import(monkeypatch):
    monkeypatch.setenv("GATEWAY_JWT_ISSUER", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCE", AUD)
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", PUB)
    assert identity.decide(user_tok(), None, None, ROUTE).refused is None
    monkeypatch.setenv("GATEWAY_JWT_ISSUER", "https://other.example")
    assert identity.decide(user_tok(), None, None, ROUTE).refused == "issuer_not_accepted"


def test_a_token_without_exp_is_not_accepted(monkeypatch):
    monkeypatch.setenv("GATEWAY_JWT_ISSUER", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCE", AUD)
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", PUB)
    tok = jwt.encode({"sub": "k", "iss": ISS, "aud": AUD, "email": "a@example.com"},
                     PRIV, algorithm="RS256")
    assert identity.decide(tok, None, None, ROUTE).refused == "token_invalid"


# ---------------------------------------------------------------------------
# OBSERVE — outcomes are what today's code yields, header honoured as before
# ---------------------------------------------------------------------------
def _observe_tokens():
    return {
        # kind: (token builder, the claim today's code would fall back to)
        "valid_user": (lambda: user_tok("alice@example.com"), "alice@example.com"),
        "valid_service": (lambda: svc_tok("svc:etl"), "svc:etl"),
        "forged_user": (lambda: user_tok("alice@example.com", key=OTHER_PRIV), "alice@example.com"),
        "unsigned_hs256": (lambda: jwt.encode({"sub": "k", "email": "alice@example.com"}, "k" * 32, algorithm="HS256"),
                           "alice@example.com"),
        "no_claim": (lambda: _tok({}), None),
    }


_COMBOS = {
    "none": ({}, "FALLBACK"),
    "on_behalf_of": ({"on_behalf_of": "bob@example.com"}, "bob@example.com"),
    "legacy": ({"X-Originator-Email": "bob@example.com"}, "bob@example.com"),
    "both_new_wins": ({"on_behalf_of": "bob@example.com", "X-Originator-Email": "carol@example.com"}, "bob@example.com"),
}


@pytest.mark.parametrize("combo", sorted(_COMBOS))
@pytest.mark.parametrize("kind", sorted(_observe_tokens()))
def test_observe_outcome_is_what_today_yields(kind, combo, world, http, monkeypatch):
    # Observe is the default; configure a verifier too, so the would-decide path runs for real.
    monkeypatch.setenv("GATEWAY_JWT_ISSUER", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCE", AUD)
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", PUB)
    mk, fallback = _observe_tokens()[kind]
    headers, want = _COMBOS[combo]
    want = fallback if want == "FALLBACK" else want
    r = call(http, mk(), **headers)
    if want is None:
        assert r.status_code == 403 and world.topaz == []
    else:
        assert r.status_code == 200, r.text
        assert world.topaz == [want]
        assert world.broker[0]["subject"] == (None if combo == "none" else want)


def test_observe_logs_what_enforce_would_do_and_a_forged_token_still_passes(world, http, monkeypatch, lines):
    monkeypatch.setenv("GATEWAY_JWT_ISSUER", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCE", AUD)
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", PUB)
    r = call(http, user_tok("alice@example.com", key=OTHER_PRIV))
    assert r.status_code == 200, "observe is today's posture: a forged token passes through"
    assert world.topaz == ["alice@example.com"]
    would = lines("IDENTITY_WOULD")
    assert would and "outcome=refuse" in would[-1] and "reason=token_invalid" in would[-1]
    assert f"urn={URN}" in would[-1]
    assert not lines("IDENTITY_AUDIT")


def test_observe_would_allow_reads_allow(world, http, monkeypatch, lines):
    monkeypatch.setenv("GATEWAY_JWT_ISSUER", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCE", AUD)
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", PUB)
    call(http, svc_tok(), on_behalf_of="alice@example.com")
    # Not a listed delegate here: the would-line says refuse, the request is still served.
    assert "outcome=refuse reason=not_a_delegate" in lines("IDENTITY_WOULD")[-1]
    rail(monkeypatch, {("svc:etl", ROUTE)})
    r = call(http, svc_tok(), on_behalf_of="alice@example.com")
    assert r.status_code == 200
    assert "outcome=allow" in lines("IDENTITY_WOULD")[-1]
    assert "subject=alice@example.com via=svc:etl" in lines("IDENTITY_WOULD")[-1]


def test_a_defect_in_decide_leaves_an_observed_request_unaffected(world, http, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("decide is broken")

    monkeypatch.setattr(identity, "decide", boom)
    r = call(http, user_tok("alice@example.com"), on_behalf_of="bob@example.com")
    assert r.status_code == 200
    assert world.topaz == ["bob@example.com"]


def test_a_defect_in_decide_under_enforce_refuses(enforce, world, http, monkeypatch):
    monkeypatch.setattr(identity, "decide", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    r = call(http, user_tok())
    assert r.status_code == 503 and world.topaz == []


def test_the_gauge_is_fed_the_effective_header_subject(world, http, monkeypatch):
    seen = {}
    real = gateway.subject_gauge.observe

    def spy(**kw):
        seen.update(kw)
        return real(**kw)

    monkeypatch.setattr(gateway.subject_gauge, "observe", spy)
    call(http, user_tok(), on_behalf_of="bob@example.com")
    assert seen["header_subject"] == "bob@example.com"


# ---------------------------------------------------------------------------
# Startup announcement and layout
# ---------------------------------------------------------------------------
def test_announce_states_posture_and_issuer_without_values(monkeypatch):
    monkeypatch.setenv("GATEWAY_IDENTITY_POSTURE", "enforce")
    monkeypatch.setenv("GATEWAY_JWT_ISSUER", ISS)
    line = identity.announce()
    assert "posture=enforce" in line and "issuers=1" in line and "delegates" not in line
    assert ISS not in line
    # Nothing is accepted yet: no audience and no key source. The line says which, BY NAME,
    # because "can this deployment turn enforce on" is the question an operator asks, and the
    # pod is the only thing that cannot answer it out of date.
    assert "enforce_ready=no" in line and identity.AUDIENCES_ENV[0] in line
    assert AUD not in line and PUB not in line


def test_the_gateway_announces_identity_at_startup():
    src = (Path(__file__).resolve().parents[1] / "dag_tools" / "central_gateway" / "main.py").read_text(encoding="utf-8")
    import re
    m = re.search(r"async def lifespan\(.*?(?=\napp = )", src, re.S)
    assert m and "identity.announce()" in m.group(0)


def test_identity_and_main_import_under_the_FLAT_container_layout(tmp_path):
    """The image copies central_gateway FLAT into /app; `identity` and `main` must import as
    top-level modules there. A bare relative import is a startup CrashLoop."""
    src = Path(__file__).resolve().parents[1] / "dag_tools" / "central_gateway"
    flat = tmp_path / "app"
    flat.mkdir()
    for f in src.glob("*.py"):  # the image does `COPY . .`: every module, not a hand-kept list
        (flat / f.name).write_text(f.read_text(encoding="utf-8"), encoding="utf-8")
    code = "import sys; sys.path.insert(0, sys.argv[1]); import identity, main; print('ok', identity.ROUTE_AUTHORIZE)"
    out = subprocess.run([sys.executable, "-I", "-c", code, str(flat)], capture_output=True, text=True,
                         cwd=str(tmp_path), timeout=120)
    assert out.returncode == 0, out.stderr[-600:]
    assert "ok assets.authorize" in out.stdout


# ---------------------------------------------------------------------------
# The client
# ---------------------------------------------------------------------------
class _Captured:
    def __init__(self):
        self.headers = None

    def install(self, status=200, body=None):
        outer = self
        payload = body if body is not None else {"source_type": "__unsupported__", "physical_uri": "x", "credentials": {}}

        class _Resp:
            status_code = status
            text = ""

            def raise_for_status(self):
                pass

            def json(self):
                return payload

        class _Client:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

            def post(self, url, headers=None, **kw):
                outer.headers = headers
                return _Resp()

        import httpx
        from unittest.mock import patch
        return patch.object(httpx, "Client", _Client)


def _client(**kw):
    from dag_tools.cortex_data.client import CortexDataClient
    from unittest.mock import patch
    with patch.dict(os.environ, {"CORTEX_BROKER_URL": "http://gw", "MESH_DEV_TOKEN": "t"}, clear=True):
        return CortexDataClient(**kw)


def test_client_sends_both_headers_with_the_same_value():
    cap = _Captured()
    c = _client(on_behalf_of="u@example.com", originator_sub="sub-1")
    with cap.install(), pytest.raises(ValueError, match="Unsupported source_type"):
        c.get_dataframe("urn:x")
    assert cap.headers["on_behalf_of"] == "u@example.com"
    assert cap.headers["X-Originator-Email"] == "u@example.com"
    assert cap.headers["X-Originator-Sub"] == "sub-1"


def test_client_alias_originator_email_feeds_the_same_rung():
    c = _client(originator_email="u@example.com")
    assert c.originator_email == "u@example.com" and c.authz_id == "u@example.com"
    assert c.identity_rung == "explicit"
    cap = _Captured()
    with cap.install(), pytest.raises(ValueError, match="Unsupported source_type"):
        c.get_dataframe("urn:x")
    assert cap.headers["on_behalf_of"] == "u@example.com"


def test_client_same_value_both_names_is_fine_and_conflict_raises():
    assert _client(on_behalf_of="u@example.com", originator_email="u@example.com").authz_id == "u@example.com"
    with pytest.raises(ValueError, match="different subjects"):
        _client(on_behalf_of="u@example.com", originator_email="v@example.com")


def test_client_sends_neither_header_when_reading_as_the_service():
    cap = _Captured()
    c = _client()
    with cap.install(), pytest.raises(ValueError, match="Unsupported source_type"):
        c.get_dataframe("urn:x")
    assert "on_behalf_of" not in cap.headers and "X-Originator-Email" not in cap.headers


@pytest.mark.parametrize("status", [401, 403, 503])
def test_client_surfaces_an_object_detail_reason_without_crashing(status):
    from dag_tools.cortex_data.identity import CortexDataError
    cap = _Captured()
    c = _client(on_behalf_of="u@example.com")
    body = {"detail": {"error": "on_behalf_of_differs", "message": "differs from the proven identity"}}
    with cap.install(status=status, body=body), pytest.raises(CortexDataError) as ei:
        c.get_dataframe("urn:x")
    assert "on_behalf_of_differs" in str(ei.value)
    assert f"HTTP {status}" in str(ei.value)


def test_client_still_handles_a_string_detail():
    from dag_tools.cortex_data.identity import CortexDataError
    cap = _Captured()
    c = _client(on_behalf_of="u@example.com")
    with cap.install(status=403, body={"detail": "Not authorized to access this asset"}), pytest.raises(CortexDataError) as ei:
        c.get_dataframe("urn:x")
    assert "Not authorized to access this asset" in str(ei.value)

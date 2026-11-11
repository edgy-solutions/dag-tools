"""The two internal doors (broker resolve, gateway register) require a service token.

Real RS256 tokens, the REAL routes through a TestClient. Every refusal asserts on the handler's
EFFECT (nothing minted, nothing registered), not only the status. The call sites are checked on
the outbound REQUEST through a real `httpx.MockTransport`.
"""
from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

jwt = pytest.importorskip("jwt")
pytest.importorskip("redis", reason="gateway tests need the [broker] extra")
pytest.importorskip("cryptography", reason="RS256 tests need cryptography")
pytest.importorskip("boto3", reason="the broker module imports boto3")

import httpx  # noqa: E402
from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from dag_tools.central_gateway import identity, internal_door  # noqa: E402
from dag_tools.central_gateway import main as gateway  # noqa: E402
from dag_tools.domain_broker import main as broker  # noqa: E402

ISS = "https://idp.example/realms/mesh"
AUD = "data-gateway"
URN = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/x,PROD)"


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
OTHER_PRIV, _ = _keypair()


def _tok(email, *, key=PRIV, exp=300, aud=AUD):
    # The doors share `identity._verify`, so a service token carries an audience too. ONE
    # verification, not a per-caller policy -- a second one is the bug class this work closes.
    return jwt.encode({"sub": "kc-1", "iss": ISS, "aud": aud, "exp": int(time.time()) + exp,
                       "email": email}, key, algorithm="RS256")


SVC_OK = "svc:gateway"
SVC_OTHER = "svc:somebody-else"


# ---------------------------------------------------------------------------
# The two doors, each with a handler EFFECT counter
# ---------------------------------------------------------------------------
class _Door(SimpleNamespace):
    pass


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for k in ("INTERNAL_DOOR_POSTURE", "BROKER_RESOLVE_CALLERS", "GATEWAY_REGISTER_CALLERS",
              "GATEWAY_JWT_ISSUER", "GATEWAY_JWT_ISSUERS", "GATEWAY_JWT_AUDIENCE",
              "GATEWAY_JWT_AUDIENCES", "GATEWAY_JWKS_URL", "GATEWAY_JWT_PUBLIC_KEY",
              "KEYCLOAK_PUBLIC_KEY", "GATEWAY_JWT_ALGORITHMS", "USER_ENTITLEMENT_CLAIM",
              "MESH_SERVICE_TOKEN_URL", "MESH_SERVICE_CLIENT_ID", "MESH_SERVICE_CLIENT_SECRET"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(identity, "_unconfigured_said", None)
    monkeypatch.setattr(identity, "_jwks_client", None)
    monkeypatch.setattr(identity, "_jwks_client_url", None)
    monkeypatch.setattr(internal_door, "_cache", {"token": None, "fetch_at": 0.0})
    monkeypatch.setattr(internal_door, "_unset_warned", False)


@pytest.fixture
def verifying(monkeypatch):
    monkeypatch.setenv("GATEWAY_JWT_ISSUER", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCE", AUD)
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", PUB)


@pytest.fixture
def resolve_door(monkeypatch):
    """The broker's resolve route. The effect is a spy MINTER: a refused call must not reach it."""
    minted = []

    def spy_minter(scope, urn, ticket, ctx):
        minted.append(urn)
        return {"aws_access_key_id": "MINTED_KEY"}

    monkeypatch.setitem(broker._MINTERS, "spy_source", (spy_minter, lambda uri: "scope"))
    monkeypatch.setitem(broker.LOCAL_ASSETS, URN, {
        "_routing_ticket": {"source_type": "spy_source", "physical_uri": "s3://b/x"}})
    c = TestClient(broker.app)
    return _Door(
        name="resolve", route=internal_door.ROUTE_RESOLVE, env="BROKER_RESOLVE_CALLERS",
        effects=minted,
        fire=lambda h: c.post("/api/v1/internal/resolve", json={"urn": URN}, headers=h),
    )


@pytest.fixture
def register_door(monkeypatch):
    """The gateway's register route. The effect is a Redis `setex`: a refused call writes no route."""
    written = []

    class _Pipe:
        def setex(self, key, ttl, value):
            written.append((key, value))

        async def execute(self):
            return []

    class _Redis:
        def pipeline(self):
            return _Pipe()

    monkeypatch.setattr(gateway, "redis_client", _Redis())
    c = TestClient(gateway.app)
    return _Door(
        name="register", route=internal_door.ROUTE_REGISTER, env="GATEWAY_REGISTER_CALLERS",
        effects=written,
        fire=lambda h: c.post("/api/v1/internal/register",
                              json={"broker_url": "http://attacker.invalid", "asset_urns": [URN]},
                              headers=h),
    )


@pytest.fixture(params=["resolve", "register"])
def door(request, monkeypatch):
    return request.getfixturevalue(f"{request.param}_door")


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


def _reason(resp):
    return resp.json()["detail"]["error"]


@pytest.fixture
def lines(caplog):
    caplog.set_level(logging.INFO)

    def grep(token):
        return [r.getMessage() for r in caplog.records if token in r.getMessage()]
    return grep


# Each refusal: (id, headers builder, status, reason)
def _refusal_cases():
    return [
        ("no_bearer", lambda: {}, 401, "no_token"),
        ("not_bearer_scheme", lambda: {"Authorization": "Basic abc"}, 401, "no_token"),
        ("garbage_token", lambda: bearer("not.a.jwt"), 401, "token_invalid"),
        ("wrong_signing_key", lambda: bearer(_tok(SVC_OK, key=OTHER_PRIV)), 401, "token_invalid"),
        ("expired", lambda: bearer(_tok(SVC_OK, exp=-60)), 401, "token_invalid"),
        ("a_user", lambda: bearer(_tok("alice@example.com")), 403, "not_a_service"),
        ("service_not_listed", lambda: bearer(_tok(SVC_OTHER)), 403, "service_not_listed"),
    ]


REFUSALS = _refusal_cases()


# ---------------------------------------------------------------------------
# enforce
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("case", REFUSALS, ids=[c[0] for c in REFUSALS])
def test_enforce_refuses_and_the_handler_is_not_reached(door, verifying, monkeypatch, case, lines):
    _, headers, status, reason = case
    monkeypatch.setenv("INTERNAL_DOOR_POSTURE", "enforce")
    monkeypatch.setenv(door.env, SVC_OK)
    resp = door.fire(headers())
    assert resp.status_code == status
    assert resp.json()["detail"] == {"error": reason, "route": door.route}
    assert door.effects == [], "the handler ran for a refused call"
    assert len(lines(f"internal_door refused route={door.route} reason={reason}")) == 1


def test_enforce_serves_a_listed_service_and_names_it(door, verifying, monkeypatch, lines):
    monkeypatch.setenv("INTERNAL_DOOR_POSTURE", "enforce")
    monkeypatch.setenv(door.env, "svc:other , " + SVC_OK)  # whitespace around names is tolerated
    resp = door.fire(bearer(_tok(SVC_OK)))
    assert resp.status_code == 200
    assert len(door.effects) == 1
    assert lines(f"internal_door served route={door.route} service={SVC_OK}")


@pytest.mark.parametrize("value", [None, "", " , "], ids=["unset", "empty", "blanks"])
def test_enforce_with_no_list_refuses_everyone(door, verifying, monkeypatch, value):
    monkeypatch.setenv("INTERNAL_DOOR_POSTURE", "enforce")
    if value is not None:
        monkeypatch.setenv(door.env, value)
    resp = door.fire(bearer(_tok(SVC_OK)))
    assert resp.status_code == 403 and _reason(resp) == "service_not_listed"
    assert door.effects == []


def test_a_list_for_one_door_does_not_open_the_other(verifying, monkeypatch, resolve_door, register_door):
    monkeypatch.setenv("INTERNAL_DOOR_POSTURE", "enforce")
    monkeypatch.setenv("GATEWAY_REGISTER_CALLERS", SVC_OK)
    assert resolve_door.fire(bearer(_tok(SVC_OK))).status_code == 403
    assert register_door.fire(bearer(_tok(SVC_OK))).status_code == 200


def test_enforce_unconfigured_verifier_is_503(door, monkeypatch):
    monkeypatch.setenv("INTERNAL_DOOR_POSTURE", "enforce")
    monkeypatch.setenv(door.env, SVC_OK)  # no issuer, no key
    resp = door.fire(bearer(_tok(SVC_OK)))
    assert resp.status_code == 503 and _reason(resp) == "verification_unconfigured"
    assert door.effects == []


def test_enforce_jwks_outage_is_503_for_that_request_only_no_latch(door, monkeypatch):
    monkeypatch.setenv("INTERNAL_DOOR_POSTURE", "enforce")
    monkeypatch.setenv(door.env, SVC_OK)
    monkeypatch.setenv("GATEWAY_JWT_ISSUER", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCE", AUD)
    monkeypatch.setenv("GATEWAY_JWKS_URL", "https://idp.example/jwks")
    state = {"down": True}

    def signing_key(url, token):
        if state["down"]:
            raise identity._JwksUnavailable("PyJWKClientConnectionError")
        return PUB

    monkeypatch.setattr(identity, "_jwks_signing_key", signing_key)
    resp = door.fire(bearer(_tok(SVC_OK)))
    assert resp.status_code == 503 and _reason(resp) == "jwks_unavailable"
    assert door.effects == []
    state["down"] = False
    assert door.fire(bearer(_tok(SVC_OK))).status_code == 200  # the next request tries again
    assert len(door.effects) == 1


def test_the_refusal_never_echoes_or_logs_the_token(door, verifying, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("INTERNAL_DOOR_POSTURE", "enforce")
    monkeypatch.setenv(door.env, SVC_OK)
    for tok in (_tok(SVC_OTHER), _tok("alice@example.com"), _tok(SVC_OK, key=OTHER_PRIV)):
        resp = door.fire(bearer(tok))
        assert tok not in resp.text
        assert tok not in caplog.text
        assert tok.split(".")[1] not in caplog.text  # nor its claim set
    assert "alice@example.com" not in caplog.text  # a user is not named in the log


# ---------------------------------------------------------------------------
# observe (the default) and the posture parse
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("case", REFUSALS, ids=[c[0] for c in REFUSALS])
@pytest.mark.parametrize("setting", [None, "observe", ""], ids=["default", "observe", "blank"])
def test_observe_serves_and_logs_would_refuse(door, verifying, monkeypatch, case, setting, lines):
    _, headers, _status, reason = case
    if setting is not None:
        monkeypatch.setenv("INTERNAL_DOOR_POSTURE", setting)
    monkeypatch.setenv(door.env, SVC_OK)
    resp = door.fire(headers())
    assert resp.status_code == 200
    assert len(door.effects) == 1
    assert len(lines(f"internal_door would_refuse route={door.route} reason={reason}")) == 1
    assert not lines("internal_door refused")


def test_observe_logs_would_refuse_for_an_unconfigured_verifier(door, monkeypatch, lines):
    monkeypatch.setenv(door.env, SVC_OK)
    assert door.fire(bearer(_tok(SVC_OK))).status_code == 200
    assert lines("would_refuse") and "reason=verification_unconfigured" in lines("would_refuse")[0]


def test_observe_listed_service_logs_no_would_refuse(door, verifying, monkeypatch, lines):
    monkeypatch.setenv(door.env, SVC_OK)
    assert door.fire(bearer(_tok(SVC_OK))).status_code == 200
    assert not lines("would_refuse")


def test_an_unknown_posture_is_enforce_and_says_so(door, verifying, monkeypatch, caplog):
    monkeypatch.setenv("INTERNAL_DOOR_POSTURE", "enforcing")
    monkeypatch.setenv(door.env, SVC_OK)
    assert door.fire({}).status_code == 401
    assert "INTERNAL_DOOR_POSTURE has an unrecognised value" in caplog.text
    assert door.effects == []


def test_the_posture_is_independent_of_the_identity_posture(door, verifying, monkeypatch):
    monkeypatch.setenv("GATEWAY_IDENTITY_POSTURE", "enforce")
    monkeypatch.setenv(door.env, SVC_OK)
    assert door.fire({}).status_code == 200  # doors still observe
    monkeypatch.delenv("GATEWAY_IDENTITY_POSTURE")
    monkeypatch.setenv("INTERNAL_DOOR_POSTURE", "enforce")
    assert identity.posture() == "observe"
    assert door.fire({}).status_code == 401


# ---------------------------------------------------------------------------
# service_headers
# ---------------------------------------------------------------------------
class _TokenEndpoint:
    """A real `httpx.MockTransport` behind every `httpx.AsyncClient`; records each request."""

    def __init__(self, monkeypatch, routes=None):
        self.requests = []
        self.clock = [1000.0]
        self.expires_in = 300
        self.fail = False
        self.n = 0
        self.routes = routes or {}
        real = httpx.AsyncClient

        def handler(request: httpx.Request):
            self.requests.append(request)
            if request.url.host == "idp.invalid":
                if self.fail:
                    return httpx.Response(500, json={"error": "boom"})
                self.n += 1
                return httpx.Response(200, json={"access_token": f"SVC-TOKEN-{self.n}",
                                                 "expires_in": self.expires_in})
            return self.routes.get(request.url.path, lambda r: httpx.Response(200, json={}))(request)

        monkeypatch.setattr(httpx, "AsyncClient",
                            lambda *a, **k: real(*a, **{**k, "transport": httpx.MockTransport(handler)}))
        monkeypatch.setattr(internal_door.time, "monotonic", lambda: self.clock[0])

    def token_calls(self):
        return [r for r in self.requests if r.url.host == "idp.invalid"]


@pytest.fixture
def mesh_creds(monkeypatch):
    monkeypatch.setenv("MESH_SERVICE_TOKEN_URL", "https://idp.invalid/token")
    monkeypatch.setenv("MESH_SERVICE_CLIENT_ID", "svc-client")
    monkeypatch.setenv("MESH_SERVICE_CLIENT_SECRET", "s3cr3t-value")


def test_service_headers_does_a_client_credentials_grant_and_caches(monkeypatch, mesh_creds):
    ep = _TokenEndpoint(monkeypatch)
    h1 = asyncio.run(internal_door.service_headers())
    h2 = asyncio.run(internal_door.service_headers())
    assert h1 == h2 == {"Authorization": "Bearer SVC-TOKEN-1"}
    calls = ep.token_calls()
    assert len(calls) == 1
    body = calls[0].content.decode()
    assert "grant_type=client_credentials" in body and "client_id=svc-client" in body
    assert "client_secret=s3cr3t-value" in body


def test_service_headers_refetches_30s_before_expiry(monkeypatch, mesh_creds):
    ep = _TokenEndpoint(monkeypatch)
    assert asyncio.run(internal_door.service_headers())["Authorization"].endswith("-1")
    ep.clock[0] += 269  # 1s inside the window
    assert asyncio.run(internal_door.service_headers())["Authorization"].endswith("-1")
    ep.clock[0] += 2    # 271s: past expires_in - 30
    assert asyncio.run(internal_door.service_headers())["Authorization"].endswith("-2")
    assert len(ep.token_calls()) == 2


def test_service_headers_unset_is_empty_and_errors_once(monkeypatch, caplog):
    ep = _TokenEndpoint(monkeypatch)
    for _ in range(3):
        assert asyncio.run(internal_door.service_headers()) == {}
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    for name in ("MESH_SERVICE_TOKEN_URL", "MESH_SERVICE_CLIENT_ID", "MESH_SERVICE_CLIENT_SECRET"):
        assert name in errors[0].getMessage()
    assert ep.requests == []


def test_service_headers_fetch_failure_is_empty_logs_the_type_and_retries(monkeypatch, mesh_creds, caplog):
    ep = _TokenEndpoint(monkeypatch)
    ep.fail = True
    assert asyncio.run(internal_door.service_headers()) == {}
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and "HTTPStatusError" in errors[0]
    assert "boom" not in caplog.text and "s3cr3t-value" not in caplog.text
    ep.fail = False
    assert asyncio.run(internal_door.service_headers()) == {"Authorization": "Bearer SVC-TOKEN-1"}  # retried at once
    assert len(ep.token_calls()) == 2


def test_service_headers_survives_a_transport_error(monkeypatch, mesh_creds, caplog):
    def boom(request):
        raise httpx.ConnectError("no route", request=request)
    real = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient",
                        lambda *a, **k: real(*a, **{**k, "transport": httpx.MockTransport(boom)}))
    assert asyncio.run(internal_door.service_headers()) == {}
    assert "ConnectError" in caplog.text


def test_the_service_token_never_reaches_the_log(monkeypatch, mesh_creds, caplog):
    caplog.set_level(logging.DEBUG)
    _TokenEndpoint(monkeypatch)
    asyncio.run(internal_door.service_headers())
    asyncio.run(internal_door.service_headers())
    assert "SVC-TOKEN" not in caplog.text and "s3cr3t-value" not in caplog.text


# ---------------------------------------------------------------------------
# The two call sites send the header (asserted on the outbound request)
# ---------------------------------------------------------------------------
def test_the_gateway_proxy_sends_the_service_token_to_the_broker(monkeypatch, mesh_creds):
    seen = {}

    def broker_reply(request):
        seen["auth"] = request.headers.get("authorization")
        seen["x"] = request.headers.get("x-keep")
        return httpx.Response(200, json={"source_type": "s3_parquet", "physical_uri": "s3://b/x"})

    ep = _TokenEndpoint(monkeypatch, routes={"/api/v1/internal/resolve": broker_reply})

    class _Redis:
        async def get(self, key):
            return "http://broker.invalid"

    monkeypatch.setattr(gateway, "redis_client", _Redis())

    async def allow(*a, **k):
        return True, None, None

    monkeypatch.setattr(gateway, "check_topaz_authz", allow)
    # The header names the principal. This module is about the internal DOOR, not about
    # identity resolution, and a request naming nobody is refused before the proxy is reached.
    resp = TestClient(gateway.app).post(
        f"/api/v1/assets/{URN}/authorize",
        headers={**bearer(_tok("alice@example.com")), "on_behalf_of": "alice@example.com"})
    assert resp.status_code == 200, resp.text
    assert seen["auth"] == "Bearer SVC-TOKEN-1"
    assert len(ep.token_calls()) == 1


def test_the_gateway_proxy_still_calls_when_no_credentials_are_set(monkeypatch):
    seen = {}

    def broker_reply(request):
        seen["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json={"source_type": "s3_parquet", "physical_uri": "s3://b/x"})

    _TokenEndpoint(monkeypatch, routes={"/api/v1/internal/resolve": broker_reply})

    class _Redis:
        async def get(self, key):
            return "http://broker.invalid"

    monkeypatch.setattr(gateway, "redis_client", _Redis())

    async def allow(*a, **k):
        return True, None, None

    monkeypatch.setattr(gateway, "check_topaz_authz", allow)
    # The header names the principal. This module is about the internal DOOR, not about
    # identity resolution, and a request naming nobody is refused before the proxy is reached.
    resp = TestClient(gateway.app).post(
        f"/api/v1/assets/{URN}/authorize",
        headers={**bearer(_tok("alice@example.com")), "on_behalf_of": "alice@example.com"})
    assert resp.status_code == 200
    assert seen["auth"] is None


def test_the_broker_register_call_sends_the_service_token(monkeypatch, mesh_creds):
    seen = {}

    def gateway_reply(request):
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = request.content
        return httpx.Response(200, json={"status": "success"})

    ep = _TokenEndpoint(monkeypatch, routes={"/api/v1/internal/register": gateway_reply})
    monkeypatch.setattr(broker, "CENTRAL_GATEWAY_URL", "http://gateway.invalid")
    monkeypatch.setattr(broker, "LOCAL_ASSETS", {URN: {}})

    async def go():
        async with httpx.AsyncClient(timeout=10.0) as client:
            await broker._register_once(client)

    asyncio.run(go())
    assert seen["auth"] == "Bearer SVC-TOKEN-1"
    assert URN.encode() in seen["body"]
    assert len(ep.token_calls()) == 1


# ---------------------------------------------------------------------------
# The gateway image is FLAT: this module must import there
# ---------------------------------------------------------------------------
def test_internal_door_and_main_import_under_the_FLAT_gateway_layout(tmp_path):
    """The gateway image builds from `central_gateway/` alone (`COPY . .`) and runs `main:app` with
    every module top-level. `dag_tools` is blocked in the child so an absolute `dag_tools...`
    import (which works in this venv and nowhere in the image) goes red, as does a bare
    relative import with no fallback."""
    src = Path(__file__).resolve().parents[1] / "dag_tools" / "central_gateway"
    flat = tmp_path / "app"
    flat.mkdir()
    for f in src.glob("*.py"):
        (flat / f.name).write_text(f.read_text(encoding="utf-8"), encoding="utf-8")
    code = (
        "import sys; sys.path.insert(0, sys.argv[1]); sys.modules['dag_tools'] = None; "
        "import internal_door, main; "
        "print('ok', internal_door.ROUTE_RESOLVE, callable(internal_door.door('r', 'E')))"
    )
    out = subprocess.run([sys.executable, "-I", "-c", code, str(flat)], capture_output=True, text=True,
                         cwd=str(tmp_path), timeout=180)
    assert out.returncode == 0, out.stderr[-800:]
    assert "ok broker.resolve True" in out.stdout

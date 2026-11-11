"""ONE function resolves the principal, and everything on the request path reads its answer.

WHY A MODULE OF ITS OWN. `test_gateway_principal_record.py` seals the BEHAVIOUR: one request,
both sides, the subject the gate decided on is the subject the record names. That is the test
that matters, and it is also the test that will be satisfied again by accident -- two
resolutions agree most of the time, which is exactly how the original defect survived review
and ran in production. The divergence showed up only in the one column where the two readings
differed, and nothing in the code said there were two readings at all.

So these pins are structural, and they are about the SHAPE rather than the outcome:

  * the gate is HANDED the principal and reads nothing from the token itself;
  * the route resolves ONCE, and does not emit the evidence line separately -- that happens
    inside the one resolution, beside the decision it is evidence of;
  * one request verifies the token ONCE. Verifying twice is a second answer waiting to differ
    from the first, and with JWKS it is a second network fetch on every read as well;
  * whatever the one resolution returns is what both sides use -- asserted by making it return
    something no other code path could have produced.

A source pin is used where a runtime pin cannot see the difference, and the AST is walked
rather than the text grepped: `check_topaz_authz`'s own docstring explains that it performs no
`jwt.decode`, and a pin that a comment can break is a pin that gets deleted.
"""
from __future__ import annotations

import ast
import pathlib
import time

import pytest

jwt = pytest.importorskip("jwt")
pytest.importorskip("redis", reason="gateway tests need the [broker] extra")
pytest.importorskip("cryptography", reason="RS256 tests need cryptography")

from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from dag_tools.central_gateway import identity  # noqa: E402
from dag_tools.central_gateway import main as gateway  # noqa: E402

ISS = "https://idp.example/realms/mesh"
AUD = "data-gateway"
URN = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/x,PROD)"
ALICE = "alice@example.com"
GATEWAY_SRC = pathlib.Path(gateway.__file__)


def _keypair():
    k = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return (
        k.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode(),
        k.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode(),
    )


PRIV, PUB = _keypair()


def _tok(email=ALICE):
    return jwt.encode(
        {"sub": "kc-1", "iss": ISS, "aud": AUD, "exp": int(time.time()) + 300, "email": email},
        PRIV,
        algorithm="RS256",
    )


# ---------------------------------------------------------------------------
# Structural: where the resolution is allowed to happen
# ---------------------------------------------------------------------------
def _module_ast() -> ast.Module:
    return ast.parse(GATEWAY_SRC.read_text(encoding="utf-8"))


def _function(name: str) -> ast.AST:
    for node in ast.walk(_module_ast()):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in {GATEWAY_SRC.name} — this pin measures nothing")


def _calls(node: ast.AST) -> list[str]:
    """Every call in `node`, as a dotted name, with docstrings excluded.

    Docstrings are not code, and these pins are about code: the gate's own docstring names
    `jwt.decode` to say it does not do that, and a text search cannot tell the two apart.
    """
    body = list(getattr(node, "body", []))
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        body = body[1:]

    names = []
    for stmt in body:
        for sub in ast.walk(stmt):
            if isinstance(sub, ast.Call):
                name = _dotted(sub.func)
                if name:
                    names.append(name)
    return names


def _dotted(node: ast.AST) -> str:
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _references(node: ast.AST) -> set:
    """Every dotted name `node` mentions, called or not.

    `_principal` hands `identity.resolve_principal` to `asyncio.to_thread` rather than calling
    it -- the verification is blocking and the route is async -- so a pin that only looked at
    call targets would miss the one reference it exists to find.
    """
    return {
        _dotted(sub)
        for sub in ast.walk(node)
        if isinstance(sub, (ast.Attribute, ast.Name)) and _dotted(sub)
    }


def test_the_gate_is_handed_the_principal_and_reads_nothing_from_the_token():
    """`check_topaz_authz` used to decode the token and fall back to a claim. That decode WAS
    the second resolution: it ran on the token the request presented, with no reference to the
    decision the route had already made, so the two could only agree by coincidence.
    """
    calls = _calls(_function("check_topaz_authz"))
    assert "jwt.decode" not in calls, (
        "the gate decodes the token again — that is the second resolution returning"
    )
    assert not [c for c in calls if c.endswith("entitlement_claim")], (
        "the gate reads a claim name, so it is resolving a subject rather than being handed one"
    )
    assert not [c for c in calls if c.endswith("resolve_principal") or c.endswith("decide")], (
        "the gate resolves the principal itself; it must receive the route's one answer"
    )


def test_the_route_resolves_exactly_once_and_emits_no_evidence_line_of_its_own():
    """Two things, and the second is what keeps the first honest.

    The route resolves once -- through `_principal`, which wraps the one resolution and refuses
    a request that names nobody. And it does NOT call `identity.would` / `identity.audit`
    itself: the evidence line is emitted inside `resolve_principal`, beside the decision it is
    evidence of. Emitted separately, it is derived from its own reading of the request, which
    is how an audit trail starts describing something other than what happened.
    """
    route = _calls(_function("authorize_asset"))
    assert route.count("_principal") == 1, (
        f"the route resolves the principal {route.count('_principal')} times, not once"
    )
    for emitted in ("identity.would", "identity.audit", "identity.decide"):
        assert emitted not in route, (
            f"the route calls {emitted} directly — the evidence and the decision have parted"
        )

    assert "identity.resolve_principal" in _references(_function("_principal")), (
        "_principal no longer reads the one resolution"
    )


# ---------------------------------------------------------------------------
# Runtime: one verification, one answer, read by both sides
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
    monkeypatch.setattr(identity, "_unconfigured_said", None, raising=False)
    monkeypatch.setattr(identity, "_jwks_client", None, raising=False)
    monkeypatch.setattr(identity, "_jwks_client_url", None, raising=False)
    monkeypatch.setattr(gateway, "DENIED_USER_SUBS", set())
    monkeypatch.setenv("GATEWAY_JWT_ISSUERS", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCES", AUD)
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", PUB)


@pytest.fixture
def mesh(monkeypatch):
    """Topaz and the broker, both recorded. The REAL gate runs over them."""
    seen = {"topaz": [], "broker": []}

    class _Redis:
        async def get(self, key):
            return "http://broker.invalid"

    class _Resp:
        def __init__(self, body):
            self.status_code, self._b, self.text = 200, body, ""

        def json(self):
            return self._b

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None, timeout=None):
            if "/authz/is" in url:
                seen["topaz"].append((json or {}).get("resource_context", {}).get("user_id"))
                return _Resp({"decisions": [{"is": True}]})
            seen["broker"].append(json)
            return _Resp({"source_type": "s3_parquet", "physical_uri": "s3://b/x"})

    monkeypatch.setattr(gateway, "redis_client", _Redis())
    monkeypatch.setattr(gateway.httpx, "AsyncClient", lambda *a, **k: _Client())
    return seen


def _authorize(**headers):
    h = {"Authorization": f"Bearer {_tok()}"}
    h.update({k.replace("_", "-") if k.startswith("X") else k: v for k, v in headers.items()})
    return TestClient(gateway.app).post(f"/api/v1/assets/{URN}/authorize", headers=h)


def test_one_request_verifies_the_token_ONCE(mesh, monkeypatch):
    """A second verification is a second answer waiting to differ from the first.

    It is also a second network call: with `GATEWAY_JWKS_URL` configured, every verification
    can reach the IdP's key endpoint, so a duplicate makes the IdP part of every read's
    latency and part of its failure modes. The decision and the evidence line therefore share
    one verification by construction -- `resolve_principal` verifies, then hands the claims to
    the decision, rather than each arm verifying for itself.
    """
    calls = []
    real = identity._verify

    def counting(token):
        calls.append(token)
        return real(token)

    monkeypatch.setattr(identity, "_verify", counting)
    assert _authorize().status_code == 200
    assert len(calls) == 1, f"the token was verified {len(calls)} times in one request"


def test_both_sides_read_WHATEVER_the_one_resolution_RETURNED(mesh, monkeypatch):
    """Make the resolution answer something no other path could produce, and look for it.

    The subject here is not in the token, not in a header, and not derivable from anything on
    the request -- so if the gate or the record resolved a subject of its own, it would be
    `alice@example.com` or nothing, and this fails. It is the sharpest available statement of
    "they read the one answer" rather than "they happen to agree".
    """
    sentinel = "resolved-by-the-one-function@example.invalid"
    monkeypatch.setattr(
        identity,
        "resolve_principal",
        lambda *a, **k: identity.IdentityDecision(sentinel, None, None, None, "kc-1", "header"),
    )

    assert _authorize().status_code == 200
    assert mesh["topaz"] == [sentinel], "the GATE did not read the one resolution"
    assert mesh["broker"][0]["subject"] == sentinel, "the RECORD did not read the one resolution"


def test_a_refusal_from_the_one_resolution_reaches_neither_topaz_nor_the_broker(mesh, monkeypatch):
    """Refused before the broker is called: no ticket is minted and no role is minted.

    Sited at the resolution rather than at the gate on purpose. A refusal the gate issues has
    already asked Topaz about somebody, and a `mint-role` backend encodes the recorded subject
    into a role's GRANTs and ROW POLICY -- so "we denied it afterwards" is not the same as
    never having named a principal at all.
    """
    monkeypatch.setattr(
        identity, "resolve_principal", lambda *a, **k: identity._refuse(identity.NO_PRINCIPAL)
    )
    resp = _authorize()
    assert resp.status_code == 401
    assert resp.json()["detail"]["error"] == identity.NO_PRINCIPAL
    assert mesh["topaz"] == [] and mesh["broker"] == []


def test_the_refusal_names_no_credential_and_says_what_to_DO():
    """401, not 403: the answer is "present credentials", not "you may not read this".

    An operator reading a 403 goes looking for a policy decision that was never made. The
    message says which two things were absent, and names neither a token nor a claim value --
    it travels wherever a response body travels.
    """
    message = identity.message_for(identity.NO_PRINCIPAL)
    assert identity._refuse(identity.NO_PRINCIPAL).status == 401
    assert "on_behalf_of" in message and "verified" in message
    assert ALICE not in message and "Bearer" not in message

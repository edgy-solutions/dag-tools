"""A read authorized for somebody is recorded against nobody, and nothing said so.

THE FINDING, and it is not reachable from either side on its own. The gateway sends the broker
a `subject` and logs the same expression in `TICKET_ISSUED` -- those two agree by construction,
deliberately, so an audit trail cannot describe someone other than the decision did. But the
subject the GATE decides on is resolved separately, and under the observe posture the two
resolutions differ:

    gate   : header, falling back to the token's own claim
    record : header, with NO fallback

So a request carrying a token that names its subject and no `X-Originator-Email` /
`on_behalf_of` header is authorized for that principal and recorded as nobody. The broker gets
``"subject": null`` and the issue line prints ``subject=-``.

That is `subject_gauge`'s `token-claim` bucket -- the one its own documentation calls the
HEALTHY shape. The healthiest way for identity to arrive is the one way the record of the read
names nobody, which is why nothing flagged it: every other signal on that request is green.

`test_a_read_authorized_for_the_tokens_own_subject_is_recorded_against_nobody` is the
load-bearing test here. It drives the real route with the REAL `check_topaz_authz` -- the other
gateway suites stub that out, which is exactly why they cannot see this -- and asserts on both
sides at once: the identity Topaz was asked about, and the subject the broker was handed. A
test that only read the gauge's own arithmetic would pass whatever the gateway did.

The gauge measures and changes nothing. Under enforce both sides read the verified decision and
the question does not arise; under observe the fix is one expression, and the reason it is not
applied here is in `principal_gauge`'s docstring: the recorded subject is encoded into a role's
GRANTs and ROW POLICY by a `mint-role` backend, so sending one where `null` went before changes
what a credential grants, for callers nobody has enumerated yet.
"""
from __future__ import annotations

import json
import logging
import time

import pytest

jwt = pytest.importorskip("jwt")
pytest.importorskip("redis", reason="gateway tests need the [broker] extra")
pytest.importorskip("cryptography", reason="RS256 tests need cryptography")
pytest.importorskip("boto3", reason="the broker module imports boto3")

import httpx  # noqa: E402
from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from dag_tools.central_gateway import identity, principal_gauge, subject_gauge  # noqa: E402
from dag_tools.central_gateway import main as gateway  # noqa: E402

ISS = "https://idp.example/realms/mesh"
URN = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/x,PROD)"
ALICE = "alice@example.com"
BOB = "bob@example.com"


def _keypair():
    k = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return (
        k.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode(),
        k.public_key()
        .public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        .decode(),
    )


PRIV, PUB = _keypair()


def _tok(email):
    return jwt.encode(
        {"sub": "kc-1", "iss": ISS, "exp": int(time.time()) + 300, "email": email},
        PRIV,
        algorithm="RS256",
    )


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for k in (
        "GATEWAY_IDENTITY_POSTURE", "GATEWAY_JWT_ISSUER", "GATEWAY_JWT_ISSUERS",
        "GATEWAY_JWT_AUDIENCE", "GATEWAY_JWT_AUDIENCES", "GATEWAY_JWKS_URL",
        "GATEWAY_JWT_PUBLIC_KEY", "KEYCLOAK_PUBLIC_KEY", "GATEWAY_JWT_ALGORITHMS",
        "USER_ENTITLEMENT_CLAIM", "DENIED_USER_SUBS", "MESH_SERVICE_TOKEN_URL",
        "MESH_SERVICE_CLIENT_ID", "MESH_SERVICE_CLIENT_SECRET",
    ):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(identity, "_unconfigured_said", None, raising=False)
    monkeypatch.setattr(identity, "_jwks_client", None, raising=False)
    monkeypatch.setattr(identity, "_jwks_client_url", None, raising=False)
    monkeypatch.setattr(subject_gauge, "_jwks_client", None, raising=False)
    monkeypatch.setattr(subject_gauge, "_jwks_failed", False, raising=False)


class _Mesh:
    """Topaz and the broker behind a real `httpx.MockTransport`, both recorded.

    `check_topaz_authz` is left REAL. Stubbing it is what makes every other gateway suite blind
    to this: the subject the gate resolves is computed inside it.
    """

    def __init__(self, monkeypatch, *, allow=True):
        self.topaz = []
        self.resolve = []
        real = httpx.AsyncClient

        def handler(request: httpx.Request):
            if request.url.path.endswith("/authz/is"):
                self.topaz.append(request)
                return httpx.Response(200, json={"decisions": [{"is": allow}]})
            if request.url.path == "/api/v1/internal/resolve":
                self.resolve.append(request)
                return httpx.Response(
                    200, json={"source_type": "s3_parquet", "physical_uri": "s3://b/x"}
                )
            return httpx.Response(404, json={})

        monkeypatch.setattr(
            httpx,
            "AsyncClient",
            lambda *a, **k: real(*a, **{**k, "transport": httpx.MockTransport(handler)}),
        )

        class _Redis:
            async def get(self, key):
                return "http://broker.invalid"

        monkeypatch.setattr(gateway, "redis_client", _Redis())

    def decided_subject(self):
        """The identity the gate actually asked Topaz about."""
        assert self.topaz, "the gate never reached Topaz"
        return json.loads(self.topaz[-1].content)["resource_context"]["user_id"]

    def recorded_subject(self):
        """The subject the broker was handed -- what the TICKET_ISSUED line reads too."""
        assert self.resolve, "the broker was never called"
        return json.loads(self.resolve[-1].content)["subject"]


def _authorize(headers):
    return TestClient(gateway.app).post(f"/api/v1/assets/{URN}/authorize", headers=headers)


def _bearer(email, **extra):
    return {"Authorization": f"Bearer {_tok(email)}", **extra}


# ---------------------------------------------------------------------------
# The finding
# ---------------------------------------------------------------------------
def test_a_read_authorized_for_the_tokens_own_subject_is_recorded_against_nobody(
    monkeypatch, caplog
):
    """This pins the behaviour as it is today, and it WILL fail the day the record gains the
    gate's fallback. That is the point of writing it this way: the change it would break is
    exactly the one that must not be made quietly, and a red test is where that conversation
    happens.
    """
    mesh = _Mesh(monkeypatch)
    with caplog.at_level(logging.INFO):
        resp = _authorize(_bearer(ALICE))          # no originator header

    assert resp.status_code == 200, resp.text
    # The decision was made FOR somebody...
    assert mesh.decided_subject() == ALICE
    # ...and the record of it names nobody. Both halves, in one test, on one request.
    assert mesh.recorded_subject() is None
    assert "subject=-" in caplog.text              # the TICKET_ISSUED line
    assert "PRINCIPAL UNRECORDED" in caplog.text
    assert "principal-record: record=unnamed" in caplog.text


def test_a_header_named_read_is_recorded_as_the_principal_it_was_decided_for(
    monkeypatch, caplog
):
    """The control. Without it, an instrument reporting `unnamed` always would pass above."""
    mesh = _Mesh(monkeypatch)
    with caplog.at_level(logging.INFO):
        resp = _authorize(_bearer(ALICE, **{"on_behalf_of": BOB}))

    assert resp.status_code == 200, resp.text
    assert mesh.decided_subject() == BOB
    assert mesh.recorded_subject() == BOB
    assert "principal-record: record=named" in caplog.text
    assert "PRINCIPAL UNRECORDED" not in caplog.text


def test_the_legacy_header_is_recorded_too(monkeypatch, caplog):
    mesh = _Mesh(monkeypatch)
    with caplog.at_level(logging.INFO):
        assert _authorize(_bearer(ALICE, **{"X-Originator-Email": BOB})).status_code == 200
    assert mesh.recorded_subject() == BOB
    assert "principal-record: record=named" in caplog.text


# ---------------------------------------------------------------------------
# It measures. It cannot refuse, and it cannot read clean when it is blind.
# ---------------------------------------------------------------------------
def test_a_gauge_that_raises_does_not_change_the_request(monkeypatch, caplog):
    """Measuring must never be able to break the thing being measured.

    One mesh, two requests. A second `_Mesh` would wrap the first one's already-patched
    `AsyncClient` and its transport would never win -- the reason this reads the same
    recorder twice rather than building a fresh one.
    """
    mesh = _Mesh(monkeypatch)
    assert _authorize(_bearer(ALICE)).status_code == 200

    def boom(**kwargs):
        raise RuntimeError("gauge defect")

    monkeypatch.setattr(principal_gauge, "observe", boom)
    with caplog.at_level(logging.WARNING):
        resp = _authorize(_bearer(ALICE))

    assert resp.status_code == 200, resp.text
    assert len(mesh.resolve) == 2
    assert mesh.resolve[1].content == mesh.resolve[0].content      # byte for byte
    assert "principal-record gauge failed" in caplog.text


def test_a_dark_subject_gauge_reads_unreadable_rather_than_clean(monkeypatch, caplog):
    """Zero-because-silent and zero-because-clean are the two states this must separate."""
    def boom(**kwargs):
        raise RuntimeError("upstream gauge defect")

    monkeypatch.setattr(subject_gauge, "observe", boom)
    mesh = _Mesh(monkeypatch)
    with caplog.at_level(logging.INFO):
        assert _authorize(_bearer(ALICE, **{"on_behalf_of": BOB})).status_code == 200

    # The request would have classified `named`; with no reading it must not.
    assert mesh.recorded_subject() == BOB
    assert "principal-record: record=unreadable" in caplog.text
    assert "principal-record: record=named" not in caplog.text


def test_nothing_is_measured_for_a_read_that_was_denied(monkeypatch, caplog):
    """Sited after the gate, so every reading is about a ticket that was actually issued."""
    mesh = _Mesh(monkeypatch, allow=False)
    with caplog.at_level(logging.INFO):
        assert _authorize(_bearer(ALICE)).status_code == 403
    assert not mesh.resolve
    assert "principal-record:" not in caplog.text


# ---------------------------------------------------------------------------
# The buckets the route cannot currently reach, pinned so they stay distinct
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "recorded, decided, bucket",
    [
        (ALICE, ALICE, principal_gauge.REC_NAMED),
        (None, ALICE, principal_gauge.REC_UNNAMED),
        ("", ALICE, principal_gauge.REC_UNNAMED),
        ("   ", ALICE, principal_gauge.REC_UNNAMED),
        (BOB, ALICE, principal_gauge.REC_DIVERGENT),
        (ALICE, None, principal_gauge.REC_UNDECIDED),
        (None, None, principal_gauge.REC_UNDECIDED),
    ],
)
def test_classify(recorded, decided, bucket):
    assert principal_gauge.classify(recorded, decided) == bucket


def test_divergent_is_not_folded_into_unnamed(caplog):
    """A record that is WRONG is worse than one that is silent; they must not share a token."""
    with caplog.at_level(logging.INFO):
        reading = principal_gauge.observe(
            urn=URN,
            recorded=BOB,
            source_reading={"effective_subject": ALICE},
            enforcing=False,
        )
    assert reading["record"] == principal_gauge.REC_DIVERGENT
    assert "PRINCIPAL MISRECORDED" in caplog.text
    assert "PRINCIPAL UNRECORDED" not in caplog.text


def test_the_warning_names_no_principal_and_no_credential(caplog):
    """It goes wherever logs go. The urn locates the read; the id is in TICKET_ISSUED."""
    with caplog.at_level(logging.WARNING):
        principal_gauge.observe(
            urn=URN, recorded=None, source_reading={"effective_subject": ALICE}, enforcing=False
        )
    warning = next(
        r.getMessage() for r in caplog.records if "PRINCIPAL UNRECORDED" in r.getMessage()
    )
    assert ALICE not in warning


# ---------------------------------------------------------------------------
# The wiring, pinned at the source
#
# The container-layout pin is NOT duplicated here: `test_subject_source_gauge.py`'s
# `test_the_gateway_IMPORTS_under_the_layout_the_CONTAINER_actually_uses` walks the AST for
# every unguarded relative import in main.py, so this module's shim is already covered by it.
# ---------------------------------------------------------------------------
def _gateway_src() -> str:
    """Read main.py as TEXT. Importing it drags in redis/fastapi and would make these pins
    fail for reasons unrelated to what they assert; a source pin also cannot be satisfied by
    a monkeypatched attribute at runtime."""
    import pathlib

    return (
        pathlib.Path(__file__).resolve().parents[1]
        / "dag_tools"
        / "central_gateway"
        / "main.py"
    ).read_text(encoding="utf-8")


def test_the_gateway_does_not_BRANCH_on_the_principal_gauge():
    """The same rule the subject gauge is held to, and for the same reason: the moment a
    request outcome depends on the reading, this stops being an instrument and becomes an
    unreviewed enforcement path. So the call discards its return value."""
    import re

    src = _gateway_src()
    m = re.search(r"async def authorize_asset\(.*?(?=\n@app\.|\Z)", src, re.S)
    assert m, "authorize_asset not found — this pin is measuring nothing"
    body = m.group(0)

    assert "principal_gauge.observe(" in body, "the gauge must be wired into the request path"
    assert not re.search(r"=\s*principal_gauge\.observe\(", body), (
        "the gateway kept the principal-record reading — the next edit branches on it"
    )
    assert not re.search(r"if\s+principal_gauge", body), (
        "the gateway BRANCHED on the principal-record gauge"
    )


def test_the_gauge_is_announced_at_STARTUP():
    """Otherwise an empty log reads as a clean population, and the whole point of this
    instrument is the number it produces -- including when that number is zero."""
    import re

    src = _gateway_src()
    m = re.search(r"async def lifespan\(.*?(?=\napp = )", src, re.S)
    assert m and "principal_gauge.announce()" in m.group(0), (
        "the gauge is not announced at startup"
    )


def test_the_announcement_says_it_refuses_nothing(caplog):
    """A reader meeting this line for the first time should not have to go and check."""
    with caplog.at_level(logging.INFO):
        line = principal_gauge.announce()
    assert line in caplog.text
    assert "refuses nothing" in line


def test_the_gateway_REFUSES_NOTHING_on_this_gauges_account():
    """No refusal may be attributable to the instrument. Every `raise` near it would be one."""
    import re

    for m in re.finditer(r"principal_gauge[^\n]*\n(?:[^\n]*\n){0,12}", _gateway_src()):
        assert "raise HTTPException" not in m.group(0), (
            "a refusal appeared next to the gauge — this instrument observes only"
        )

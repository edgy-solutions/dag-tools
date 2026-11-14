"""ONE resolution of the principal: the read is recorded against whoever it was authorized for.

THE FINDING THIS MODULE MEASURED, now closed. The gateway sends the broker a `subject` and logs
the same expression in `TICKET_ISSUED` -- those two agree by construction, deliberately, so an
audit trail cannot describe someone other than the decision did. But the subject the GATE
decided on was resolved SEPARATELY, and under the observe posture the two resolutions differed:

    gate   : header, falling back to the token's own claim
    record : header, with NO fallback

So a request carrying a token that named its subject and no `X-Originator-Email` /
`on_behalf_of` header was authorized for that principal and recorded as nobody -- the broker got
``"subject": null`` and the issue line printed ``subject=-``. That is `subject_gauge`'s
`token-claim` bucket, the one its own documentation calls the HEALTHY shape, which is why
nothing flagged it: every other signal on such a request is green.

WHAT CLOSED IT. `identity.resolve_principal` is now the one resolution, and the gate and the
record both read its answer -- there are no longer two resolutions that can disagree. The
measurement answered the question the fix turned on, too: the subject available to fall back on
was read from a token nothing had verified, and the third option was the right one. An
unverified claim names nobody, and a request naming nobody is refused rather than served under
either subject. So nothing sends a subject where ``null`` went before EXCEPT where the token
verified, which is the one case where the name was never in doubt.

WHAT THESE TESTS ARE NOW. The seal on that, in the shape the measurement was written in: the
REAL `check_topaz_authz` driven through the real route, asserting both sides of one request at
once -- the identity Topaz was asked about, and the subject the broker was handed. The other
gateway suites stub the gate out, which is exactly why they could not see the defect and cannot
see a regression of it either. A test that read only the gauge's own arithmetic would pass
whatever the gateway did.

`test_a_read_authorized_for_the_tokens_own_subject_is_recorded_as_that_subject` is the pin that
was deliberately red: it used to assert ``recorded_subject() is None`` and to say so in its own
docstring -- that it would fail the day the record gained the gate's fallback, and that a red
test is where that conversation happens. This is that day. The change is the other way round
from the one that comment anticipated: neither side has a fallback now, because neither side
resolves anything.
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
AUD = "data-gateway"
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


OTHER_PRIV, _OTHER_PUB = _keypair()


def _tok(email=None, *, key=PRIV, iss=ISS, aud=AUD):
    """A token this gateway accepts, unless a caller asks for one it does not.

    `email` of None omits the claim entirely: a token that verifies and names nobody, which is
    a different case from one that names somebody and did not verify, and the two have to stay
    told apart.
    """
    body = {"sub": "kc-1", "iss": iss, "aud": aud, "exp": int(time.time()) + 300}
    if email is not None:
        body["email"] = email
    return jwt.encode(body, key, algorithm="RS256")


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
    # Then serve the values a token is verified against. Not incidental any more: the token
    # path is open only for a token this gateway VERIFIED, so a module that configured no
    # issuer and no audience would be asserting the record's behaviour over a path that
    # refuses every request -- green, and about nothing.
    monkeypatch.setenv("GATEWAY_JWT_ISSUERS", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCES", AUD)
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", PUB)
    principal_gauge.reset_counts()


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


def _bearer(email=ALICE, **extra):
    return {"Authorization": f"Bearer {_tok(email)}", **extra}


def _reason(resp):
    detail = resp.json()["detail"]
    return detail["error"] if isinstance(detail, dict) else detail


# ---------------------------------------------------------------------------
# The seal: one resolution, read on both sides of one request
# ---------------------------------------------------------------------------
def test_a_read_authorized_for_the_tokens_own_subject_is_recorded_as_that_subject(
    monkeypatch, caplog
):
    """The pin that was red on purpose, now green for the right reason.

    A VERIFIED token carrying a subject and no header: admitted, authorized for that subject,
    and recorded against the same one. Both halves, in one test, on one request -- which is
    what made the divergence visible and is the only thing that can make its return visible.
    """
    mesh = _Mesh(monkeypatch)
    with caplog.at_level(logging.INFO):
        resp = _authorize(_bearer(ALICE))          # no originator header

    assert resp.status_code == 200, resp.text
    assert mesh.decided_subject() == ALICE
    assert mesh.recorded_subject() == ALICE, (
        "the record named somebody other than the decision was made for -- a second "
        "resolution has been reintroduced"
    )
    assert f"subject={ALICE}" in caplog.text       # the TICKET_ISSUED line
    assert "PRINCIPAL UNRECORDED" not in caplog.text
    assert "principal-record: record=named" in caplog.text


def test_an_UNVERIFIED_tokens_subject_claim_is_refused_rather_than_recorded(monkeypatch, caplog):
    """The other half of the answer: the fallback subject was never established.

    Signed by a key this gateway does not accept. Before, its claim became the principal, the
    read was authorized for `alice@example.com` on the token's own say-so, and the record of it
    named nobody. Now neither happens: the request names nobody and is refused before the gate.
    """
    mesh = _Mesh(monkeypatch)
    token = _tok(ALICE, key=OTHER_PRIV)
    with caplog.at_level(logging.INFO):
        resp = _authorize({"Authorization": f"Bearer {token}"})

    assert resp.status_code == 401 and _reason(resp) == identity.NO_PRINCIPAL, resp.text
    assert not mesh.topaz, "an unverified claim must never reach the authorizer"
    assert not mesh.resolve, "and no ticket is minted for it"
    assert "principal-record:" not in caplog.text, "nothing was issued, so nothing is measured"


def test_a_request_that_names_NOBODY_is_refused_before_the_broker(monkeypatch, caplog):
    """A token this gateway verified, carrying no subject claim, and no header either.

    Distinct from the case above and it must stay so: here nothing is wrong with the token, it
    simply does not say who the reader is, and no header does either. A read is never
    authorized for nobody, so this is refused rather than served with ``subject: null``.
    """
    mesh = _Mesh(monkeypatch)
    with caplog.at_level(logging.INFO):
        resp = _authorize(_bearer(None))           # verified, no claim, no header

    assert resp.status_code == 401 and _reason(resp) == identity.NO_PRINCIPAL, resp.text
    assert not mesh.topaz and not mesh.resolve


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
# The counts: what makes a zero mean something
# ---------------------------------------------------------------------------
def test_health_carries_the_counts_and_observed_is_the_discriminator(monkeypatch):
    """`unnamed=0` is a closed defect when `observed` is large and a dark gauge when it is 0.

    Those are the two states an instrument exists to separate, which is why this is a dict on
    `/health` rather than one number -- read after a roll, the way `telemetry_misses` is, not
    grepped for in a log.
    """
    _Mesh(monkeypatch)
    client = TestClient(gateway.app)

    before = client.get("/health").json()["principal_record"]
    assert before["observed"] == 0 and before["named"] == 0, "the fixture did not reset"

    assert _authorize(_bearer(ALICE)).status_code == 200
    after = client.get("/health").json()["principal_record"]

    assert after["observed"] == 1, "a served read was not counted"
    assert after["named"] == 1
    assert after["unnamed"] == 0 and after["divergent"] == 0


def test_every_bucket_is_always_present_even_at_zero():
    """A key that appears only once it is non-zero gets read with `.get()` and is then
    indistinguishable from one that was merely misspelled."""
    keys = set(principal_gauge.counts())
    assert keys == {"observed", *principal_gauge._BUCKETS}


def test_health_still_answers_when_the_counter_misbehaves(monkeypatch):
    """A readiness probe that 500s because an instrument misbehaved takes the pod out."""
    _Mesh(monkeypatch)
    monkeypatch.setattr(
        principal_gauge, "counts", lambda: (_ for _ in ()).throw(RuntimeError("counter defect"))
    )
    resp = TestClient(gateway.app).get("/health")
    assert resp.status_code == 200
    body = resp.json()
    # This gauge's reading, and only this gauge's: `/health` carries other instruments now, and
    # whether ONE of them failing takes another's reading off the body is the thing that has to
    # stay pinned. (The body's whole key set is pinned in `test_gateway_refusal_reasons.py`;
    # asserting it here too would make every new instrument a two-file edit for no extra cover.)
    assert body["status"] == "ok"
    assert body["principal_record"] is None
    assert body["refusal_reasons"] is not None


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
    """No refusal may be attributable to the instrument. Every `raise` near it would be one.

    The window is cut at the next `def` / route decorator, because a proximity check that runs
    past the end of the enclosing function stops asking its own question: the gauge's counts
    are served near the top of `/health`, and the next route down the file is `/ready`, which
    refuses when Redis is unreachable. That refusal has nothing to do with this instrument, and
    a pin that reported it would be read as noise and then ignored -- which is how a real one
    gets ignored with it.
    """
    import re

    for m in re.finditer(r"principal_gauge[^\n]*\n(?:[^\n]*\n){0,12}", _gateway_src()):
        window = re.split(r"\n(?=@app\.|async def |def )", m.group(0))[0]
        assert "raise HTTPException" not in window, (
            "a refusal appeared next to the gauge — this instrument observes only"
        )

"""Refusals, counted by the name of their reason -- and the one reason that was logged nowhere.

TWO things are sealed here, and they are one change because they are one defect seen from two
distances.

THE WRONG REASON. The observe posture emits `IDENTITY_WOULD`, which reports what `enforce`
WOULD have done -- `_decided`'s answer. `_decided` cannot produce `no_principal`: that reason is
minted by the observe arm of `resolve_principal`, after the line has already fired, and the
route's refusal path logs nothing of its own (it raises). So an observe-posture `no_principal`
refusal reached the caller in the HTTP body and reached nobody on this side, while the SAME
request's one log line named a different reason -- `no_subject`, usually, since a token that
verifies and names nobody is how a request ends up naming nobody. A log that describes a
decision that was not made is worse than a log with a gap in it: the gap is visible.

TWO POPULATIONS, NEVER ONE NUMBER. Under `observe` "refused" means two different things, and a
single count would describe neither:

    would_refuse -- what `enforce` would have refused. The request was SERVED anyway. The
                    migration-readiness number: what breaks the day the posture flips.
    did_refuse   -- what this gateway actually refused. The caller got a 4xx.

Collapsing them tells an operator that N requests are failing when N-k of them are being served,
or the reverse. Under `enforce` the two are one decision, so a refusal counts in both -- and that
is a fact worth a test, because it is the only state in which the two numbers agreeing is right.

WHY THESE TESTS GO THROUGH THE ROUTE. Both halves are about what one request emits and counts,
and the divergence lived in the order the arms of one function ran in. A test over the gauge's
own arithmetic would pass whatever the gateway logged.
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

from dag_tools.central_gateway import identity, principal_gauge  # noqa: E402
from dag_tools.central_gateway import refusal_gauge, subject_gauge  # noqa: E402
from dag_tools.central_gateway import main as gateway  # noqa: E402

ISS = "https://idp.example/realms/mesh"
AUD = "data-gateway"
URN = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/x,PROD)"
ALICE = "alice@example.com"


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
OTHER_PRIV, _ = _keypair()


def _tok(email=None, *, key=PRIV):
    """A token this gateway accepts, unless the caller asks for one it does not.

    `email` of None omits the claim: a token that VERIFIES and names nobody. That is the request
    this module is about, and it is a different case from a token that names somebody and did
    not verify -- the two reasons are what the two numbers separate.
    """
    body = {"sub": "kc-1", "iss": ISS, "aud": AUD, "exp": int(time.time()) + 300}
    if email is not None:
        body["email"] = email
    return jwt.encode(body, key, algorithm="RS256")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for k in (
        "GATEWAY_IDENTITY_POSTURE", "GATEWAY_JWT_ISSUER", "GATEWAY_JWT_ISSUERS",
        "GATEWAY_JWT_AUDIENCE", "GATEWAY_JWT_AUDIENCES", "GATEWAY_JWKS_URL",
        "GATEWAY_JWT_PUBLIC_KEY", "KEYCLOAK_PUBLIC_KEY", "GATEWAY_JWT_ALGORITHMS",
        "USER_ENTITLEMENT_CLAIM", "DENIED_USER_SUBS",
    ):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(identity, "_unconfigured_said", None, raising=False)
    monkeypatch.setattr(identity, "_jwks_client", None, raising=False)
    monkeypatch.setattr(identity, "_jwks_client_url", None, raising=False)
    monkeypatch.setattr(subject_gauge, "_jwks_client", None, raising=False)
    monkeypatch.setattr(subject_gauge, "_jwks_failed", False, raising=False)
    monkeypatch.setenv("GATEWAY_JWT_ISSUERS", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCES", AUD)
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", PUB)
    principal_gauge.reset_counts()
    refusal_gauge.reset_counts()

    # The route refuses before it resolves anybody when Redis is unset, so a refusal test
    # without this one asserts about a 500 and proves nothing about identity at all.
    class _Redis:
        async def get(self, key):
            return "http://broker.invalid"

    monkeypatch.setattr(gateway, "redis_client", _Redis())


class _Mesh:
    """Topaz and the broker behind a real `httpx.MockTransport`, so a served request is served."""

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


def _authorize(headers):
    return TestClient(gateway.app).post(f"/api/v1/assets/{URN}/authorize", headers=headers)


def _reason(resp):
    detail = resp.json()["detail"]
    return detail["error"] if isinstance(detail, dict) else detail


def _nameless():
    """A VERIFIED token naming nobody, and no header: the request that refuses `no_principal`."""
    return {"Authorization": f"Bearer {_tok(None)}"}


def _unverifiable_but_named():
    """A token this gateway cannot verify, with a header naming the principal.

    `enforce` would refuse it (`token_invalid`); `observe` serves it off the header. The whole
    `would_refuse` population has this shape, which is why the counts are not one number.
    """
    return {
        "Authorization": f"Bearer {_tok(ALICE, key=OTHER_PRIV)}",
        "X-Originator-Email": ALICE,
    }


# ---------------------------------------------------------------------------
# The reason that was logged nowhere
# ---------------------------------------------------------------------------
def test_no_principal_is_logged_by_its_own_name(caplog):
    """The defect, sealed: the refusal the caller got is now named on this side too.

    Before, this request's only line named `no_subject` -- `enforce`'s answer -- and the 401 the
    caller received said `no_principal`. Nothing on this side could be counted, grepped or
    alerted on by the name of the reason a read was actually denied for.
    """
    with caplog.at_level(logging.INFO):
        resp = _authorize(_nameless())

    assert resp.status_code == 401, resp.text
    assert _reason(resp) == identity.NO_PRINCIPAL
    assert "IDENTITY_REFUSED" in caplog.text, (
        "an observe-posture refusal emitted no line of its own -- the reason the caller was "
        "given is visible to the caller and to nobody here"
    )
    assert f"reason={identity.NO_PRINCIPAL}" in caplog.text


def test_the_would_line_still_reports_what_ENFORCE_would_do(caplog):
    """`would`'s contract is UNCHANGED, and the two lines naming different reasons is correct.

    The fix is a second line, not a correction of the first. `IDENTITY_WOULD` answers "what
    happens when the posture flips" and the answer here really is `no_subject`: under `enforce`
    this token is refused for carrying no subject claim, before anything asks who it names.
    Rewriting that line to say `no_principal` would have cost the migration-readiness signal to
    buy the refusal signal, and a deployment needs both.
    """
    with caplog.at_level(logging.INFO):
        _authorize(_nameless())

    would = [r for r in caplog.messages if "IDENTITY_WOULD" in r]
    refused = [r for r in caplog.messages if "IDENTITY_REFUSED" in r]
    assert len(would) == 1 and len(refused) == 1
    assert f"reason={identity.NO_SUBJECT}" in would[0]
    assert f"reason={identity.NO_PRINCIPAL}" in refused[0]


def test_the_refusal_line_names_no_credential(caplog):
    """A line that goes wherever logs go carries no token. Same rule as `audit` and `would`."""
    token = _tok(None)
    with caplog.at_level(logging.INFO):
        _authorize({"Authorization": f"Bearer {token}"})

    assert "IDENTITY_REFUSED" in caplog.text
    assert token not in caplog.text
    assert "Bearer" not in caplog.text


def test_enforce_emits_no_second_refusal_line(monkeypatch, caplog):
    """Under `enforce` the refusal is ONE decision and `audit` already carries it.

    A second line there would double-count by eye: an operator grepping refusals would see each
    one twice, and the posture would look like it was refusing twice as much as it is.
    """
    monkeypatch.setenv("GATEWAY_IDENTITY_POSTURE", "enforce")
    with caplog.at_level(logging.INFO):
        resp = _authorize(_unverifiable_but_named())

    assert resp.status_code == 401
    assert "IDENTITY_AUDIT" in caplog.text
    assert f"reason={identity.TOKEN_INVALID}" in caplog.text
    assert "IDENTITY_REFUSED" not in caplog.text


# ---------------------------------------------------------------------------
# Two populations
# ---------------------------------------------------------------------------
def test_a_would_refusal_and_a_did_refusal_are_counted_separately(monkeypatch):
    """The reason this is two maps: one request was served, the other was not.

    Request one is what `enforce` would refuse and `observe` serves off the header. Request two
    is what `observe` itself refuses. A single count would have reported two refusals, one of
    which the caller got an answer to.
    """
    _Mesh(monkeypatch)
    served = _authorize(_unverifiable_but_named())
    assert served.status_code == 200, served.text

    counts = refusal_gauge.counts()
    assert counts[refusal_gauge.WOULD][identity.TOKEN_INVALID] == 1
    assert sum(counts[refusal_gauge.DID].values()) == 0, (
        "a request that was SERVED has been counted as a refusal"
    )

    refused = _authorize(_nameless())
    assert refused.status_code == 401

    counts = refusal_gauge.counts()
    assert counts[refusal_gauge.DID][identity.NO_PRINCIPAL] == 1
    assert counts[refusal_gauge.WOULD][identity.NO_SUBJECT] == 1
    assert counts[refusal_gauge.WOULD][identity.TOKEN_INVALID] == 1
    assert counts[refusal_gauge.OBSERVED] == 2


def test_under_enforce_one_refusal_counts_in_both(monkeypatch):
    """The one state where the two numbers agreeing is the right answer.

    `enforce` has a single decision, so what it would refuse and what it did refuse are the same
    request. A deployment reading `would_refuse` after the flip is reading its real refusals.
    """
    monkeypatch.setenv("GATEWAY_IDENTITY_POSTURE", "enforce")
    assert _authorize(_unverifiable_but_named()).status_code == 401

    counts = refusal_gauge.counts()
    assert counts[refusal_gauge.WOULD][identity.TOKEN_INVALID] == 1
    assert counts[refusal_gauge.DID][identity.TOKEN_INVALID] == 1


def test_every_declared_reason_is_present_at_zero():
    """Driven off `identity.REASONS`, so a reason added there is counted the day it is added.

    A key that appears only once it is non-zero is read with `.get()` and is then indistinguish-
    able from one that was merely misspelled. The vocabulary is also not written down twice:
    `identity` installs its own, from the mapping that has to name each reason anyway.
    """
    assert refusal_gauge.vocabulary() == identity.REASONS
    counts = refusal_gauge.counts()
    for bucket in (refusal_gauge.WOULD, refusal_gauge.DID):
        for reason in identity.REASONS:
            assert counts[bucket][reason] == 0, reason
        assert counts[bucket][refusal_gauge.UNDECLARED] == 0
    assert counts[refusal_gauge.OBSERVED] == 0


def test_observed_separates_a_quiet_gateway_from_a_dark_gauge(monkeypatch):
    """Every reason at 0 means nothing until `observed` is not 0. The same discriminator as the
    other gauges, and the reason this is not just two maps of names."""
    assert refusal_gauge.counts()[refusal_gauge.OBSERVED] == 0
    _Mesh(monkeypatch)
    assert _authorize({"Authorization": f"Bearer {_tok(ALICE)}"}).status_code == 200
    counts = refusal_gauge.counts()
    assert counts[refusal_gauge.OBSERVED] == 1
    assert sum(counts[refusal_gauge.WOULD].values()) == 0
    assert sum(counts[refusal_gauge.DID].values()) == 0


def test_an_undeclared_reason_is_counted_and_warned_once(caplog):
    """A reason nothing installed lands in `undeclared`, loudly, rather than adding a key.

    It should be unreachable -- the vocabulary comes from the same mapping that gives every
    reason its status code. If it ever happens, the key sets stay stable for whoever is reading
    them and the surprise is in the log once, not once per request.
    """
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            refusal_gauge.observe(
                urn=URN, would_refused="a_reason_nobody_declared", did_refused=None,
                enforcing=False,
            )

    counts = refusal_gauge.counts()
    assert counts[refusal_gauge.WOULD][refusal_gauge.UNDECLARED] == 3
    assert "a_reason_nobody_declared" not in counts[refusal_gauge.WOULD]
    assert caplog.text.count("REFUSAL REASON UNDECLARED") == 1


# ---------------------------------------------------------------------------
# An instrument may never break the thing it measures
# ---------------------------------------------------------------------------
def test_a_defect_in_the_counter_does_not_refuse_the_read(monkeypatch, caplog):
    """The rule the gauges are built on. A read is not denied because a number went wrong."""
    _Mesh(monkeypatch)

    def boom(**_kwargs):
        raise RuntimeError("counter")

    monkeypatch.setattr(refusal_gauge, "observe", boom)
    with caplog.at_level(logging.WARNING):
        resp = _authorize({"Authorization": f"Bearer {_tok(ALICE)}"})

    assert resp.status_code == 200, resp.text
    assert "refusal-reason count failed" in caplog.text


def test_a_defect_in_the_line_does_not_silence_the_count(monkeypatch):
    """And the other way round: the two are in separate blocks on purpose.

    One block would make whichever ran first the only one that survives a defect in the other,
    and which that is would be an accident of ordering rather than a decision.
    """
    def boom(*_a, **_k):
        raise RuntimeError("line")

    monkeypatch.setattr(identity, "would", boom)
    assert _authorize(_nameless()).status_code == 401
    assert refusal_gauge.counts()[refusal_gauge.DID][identity.NO_PRINCIPAL] == 1


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------
def test_health_carries_the_reason_counts(monkeypatch):
    """A number a roll can be read against, rather than INFO lines someone has to go and grep.

    Scraping the lines is what a deployment had to do before, and a deployment that drops INFO
    counted nothing while looking exactly like one with nothing to count.
    """
    _Mesh(monkeypatch)
    _authorize(_nameless())
    body = TestClient(gateway.app).get("/health").json()
    assert body["refusal_reasons"][refusal_gauge.DID][identity.NO_PRINCIPAL] == 1
    assert body["refusal_reasons"][refusal_gauge.OBSERVED] == 1


def test_no_existing_health_key_changes(monkeypatch):
    """The probe body gains one key and changes nothing. `/health` is a liveness contract."""
    body = TestClient(gateway.app).get("/health").json()
    assert body["status"] == "ok"
    assert "principal_record" in body
    assert set(body) == {"status", "principal_record", "refusal_reasons"}


def test_health_still_answers_when_the_reason_counter_misbehaves(monkeypatch):
    """A readiness probe that 500s because a counter misbehaved takes the pod out.

    `null` says the reading is unavailable, which is a thing an operator can act on. A 502 from
    the load balancer is not.
    """
    def boom():
        raise RuntimeError("counts")

    monkeypatch.setattr(refusal_gauge, "counts", boom)
    resp = TestClient(gateway.app).get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["refusal_reasons"] is None
    assert body["status"] == "ok"
    assert body["principal_record"] is not None, (
        "one gauge failing took the other's reading off the body -- an operator would read the "
        "gap in the wrong place"
    )

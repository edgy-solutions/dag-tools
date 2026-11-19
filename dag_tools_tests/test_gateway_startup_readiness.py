"""A gateway that cannot verify a signed token is NOT READY, and says so before it says Redis.

THE FAULT. The gateway image installed the token library without its signature extra. The
library imports, the process starts, the container passes its healthcheck, and then every RS256
token is refused as invalid -- because the two failure paths inside the verifier (a JWKS key and
a static key) both raise, and both are swallowed into one refusal reason. Nothing anywhere said
the image could not verify anything.

WHERE IT IS ANSWERED, AND WHERE IT IS NOT. It is a deployment fault, not a new refusal reason:
a token this process cannot verify IS unverified, and the refusal vocabulary is a set of HTTP
statuses and a set of numbers on `/health`. So the refusal vocabulary is untouched -- asserted
here, byte for byte -- and the answer lives on the readiness probe instead. A process that
cannot verify a signed token is held out of service, which is what an orchestrator is for.

A ROUND TRIP, NOT A REGISTRY READ. The check signs a token and verifies it. Reading the
library's algorithm registry would have caught this one fault more cheaply, and it answers a
different question -- whether a NAME is registered. `test_the_check_is_a_round_trip_not_a
registry_read` is what makes the difference real: it leaves the registry claiming the algorithm
and breaks the signing, and the gateway must still refuse to be ready.

THE TWO DIRECTIONS IT MUST NOT BE WRONG IN. A partial answer is not ready, because a gateway
that verifies one of its two configured algorithms refuses every token its issuer actually
mints. And a check that BROKE leaves the gateway ready -- an instrument may never break the
thing it measures (ADR 4o-1), and a readiness gate that fails closed on its own defect takes
every pod out at once. Both are sealed, with their controls.
"""
from __future__ import annotations

import json
import logging
import socket

import pytest

jwt = pytest.importorskip("jwt")
pytest.importorskip("redis", reason="gateway tests need the [broker] extra")
pytest.importorskip("cryptography", reason="the round trip signs with cryptography")
pytest.importorskip("boto3", reason="the broker module imports boto3")

from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from dag_tools.central_gateway import identity  # noqa: E402
from dag_tools.central_gateway import main as gateway  # noqa: E402

#: The vocabulary this change may not touch. Spelled out rather than recomputed: a test that
#: derived it from the module it is guarding would pass whatever that module said.
REFUSAL_REASONS = (
    "token_absent",
    "token_invalid",
    "verification_unconfigured",
    "issuer_not_accepted",
    "jwks_unavailable",
    "no_subject",
    "on_behalf_of_differs",
    "not_a_delegate",
    "legacy_header",
    "no_principal",
)

#: What a published verdict must never carry. A PEM header and a JWT's own first bytes.
KEY_MATERIAL_MARKERS = ("BEGIN ", "PRIVATE KEY", "eyJ")


class _Redis:
    """A Redis that answers, so a not-ready verdict can only be about the signature."""

    def __init__(self):
        self.pings = 0

    async def ping(self):
        self.pings += 1
        return True

    async def close(self):
        return None


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for k in (
        "GATEWAY_JWT_ALGORITHMS",
        "GATEWAY_JWKS_URL",
        "GATEWAY_JWT_PUBLIC_KEY",
        "KEYCLOAK_PUBLIC_KEY",
        "GATEWAY_IDENTITY_POSTURE",
    ):
        monkeypatch.delenv(k, raising=False)
    # The verdict is cached for the life of the process, which is the point of it; each test
    # starts from no verdict so that "has not been asked" and "answered no" stay distinct.
    monkeypatch.setattr(identity, "_signature_check", None, raising=False)
    monkeypatch.setattr(gateway, "redis_client", _Redis())
    yield


#: Snapshotted at import, BEFORE any test replaces the registry function: reading it through
#: `jwt.algorithms.get_default_algorithms` inside a replacement is a call to the replacement.
_REGISTRY = dict(jwt.algorithms.get_default_algorithms())


def _unavailable(*names):
    """A registry that knows everything except `names` -- the shipped fault, reproduced."""
    return {k: v for k, v in _REGISTRY.items() if k not in names}


# ---------------------------------------------------------------------------
# What the probe answers
# ---------------------------------------------------------------------------


def test_a_gateway_that_can_verify_a_signed_token_is_ready(monkeypatch):
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")
    identity.run_signature_self_check()
    resp = TestClient(gateway.app).get("/ready")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ready"}


def test_a_gateway_that_cannot_verify_a_signed_token_is_not_ready(monkeypatch):
    """The shipped fault: the library is there and its signature support is not."""
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")
    monkeypatch.setattr(jwt.algorithms, "get_default_algorithms", lambda: _unavailable("RS256"))
    identity.run_signature_self_check()
    resp = TestClient(gateway.app).get("/ready")
    assert resp.status_code == 503
    detail = resp.json()["detail"]
    assert detail["ready"] is False
    assert detail["unsupported"] == ["RS256"]
    assert "pyjwt[crypto]" in detail["message"]


def test_the_check_is_a_round_trip_not_a_registry_read(monkeypatch):
    """The registry still claims RS256; signing with it does not work.

    Without this the whole mechanism could be a membership test wearing a round trip's name,
    and nothing would notice until an algorithm failed for some reason other than an absent
    backend -- which is the case the cheaper check cannot see at all.
    """
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")
    assert "RS256" in _REGISTRY  # the registry is untouched; only the signing is broken

    def _boom(*a, **k):
        raise ValueError("this library cannot actually sign")

    monkeypatch.setattr(jwt, "encode", _boom)
    check = identity.run_signature_self_check()
    assert check.ok is False
    assert check.failed == ("RS256",)
    assert check.unsupported == ()  # not the absent-backend fault, and not reported as one
    assert TestClient(gateway.app).get("/ready").status_code == 503


def test_partial_support_is_not_ready_and_names_the_algorithm(monkeypatch):
    """One of two is not "a signed token": it is every token the issuer mints, refused."""
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "HS256,RS256")
    monkeypatch.setattr(jwt.algorithms, "get_default_algorithms", lambda: _unavailable("RS256"))
    check = identity.run_signature_self_check()
    assert check.verified == ("HS256",)
    assert check.unsupported == ("RS256",)
    assert check.ok is False
    detail = TestClient(gateway.app).get("/ready").json()["detail"]
    assert detail["verified"] == ["HS256"] and detail["unsupported"] == ["RS256"]


def test_the_signature_fault_is_reported_before_redis(monkeypatch):
    """Order matters: Redis is usually fine at the same moment, and comes back by itself.

    A gateway that answered "Redis unreachable" -- or answered nothing, because Redis was up --
    would send an operator to the one suspect that is not the problem, for a fault that will
    never fix itself without a redeploy.
    """
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")
    monkeypatch.setattr(jwt.algorithms, "get_default_algorithms", lambda: _unavailable("RS256"))
    identity.run_signature_self_check()
    redis = _Redis()
    monkeypatch.setattr(gateway, "redis_client", redis)
    resp = TestClient(gateway.app).get("/ready")
    assert resp.status_code == 503
    assert resp.json()["detail"]["problem"] is None  # the per-algorithm answer, not a problem
    assert redis.pings == 0, "Redis was consulted for a fault that is not Redis's"


def test_a_verdict_that_was_never_taken_does_not_gate(monkeypatch):
    """None is "has not been asked", never "answered no" -- the lifespan is what asks."""
    assert identity.signature_self_check() is None
    assert TestClient(gateway.app).get("/ready").status_code == 200


# ---------------------------------------------------------------------------
# The states that are not a per-algorithm answer
# ---------------------------------------------------------------------------


def test_no_algorithm_configured_is_its_own_answer(monkeypatch):
    """An empty list accepts nothing, so it is the same outage with a different cause."""
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "  ,  ")
    check = identity.run_signature_self_check()
    assert check.problem == identity.NO_ALGORITHM_CONFIGURED
    assert check.ok is False
    assert "GATEWAY_JWT_ALGORITHMS" in check.message()


def test_an_absent_library_is_its_own_answer_with_its_own_message(monkeypatch):
    """Two causes, two remedies, two messages -- and the test asserts they DIFFER.

    An operator reading "install pyjwt[crypto]" when the library is absent entirely, or
    "set GATEWAY_JWT_ALGORITHMS" when it is installed without signature support, goes and does
    the wrong thing. Asserting each message in isolation would not catch them converging.
    """
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")
    real_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__

    def _no_jwt(name, *a, **k):
        if name == "jwt":
            raise ModuleNotFoundError("No module named 'jwt'")
        return real_import(name, *a, **k)

    monkeypatch.setattr("builtins.__import__", _no_jwt)
    absent = identity.check_signature_support()
    monkeypatch.undo()

    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "")
    unset = identity.check_signature_support()

    assert absent.problem == identity.LIBRARY_ABSENT and absent.ok is False
    assert unset.problem == identity.NO_ALGORITHM_CONFIGURED and unset.ok is False
    assert absent.message() != unset.message()


def test_a_defect_in_the_check_itself_does_not_take_the_pod_out(monkeypatch):
    """An instrument may never break the thing it measures.

    This is the one problem that answers READY. A check that threw is a measurement of the
    check; failing closed on it would hold every pod in the fleet out of service at once, over
    a bug in the thing that was meant to prevent an outage.
    """
    monkeypatch.setattr(
        identity, "check_signature_support", lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    check = identity.run_signature_self_check()
    assert check.problem == identity.CHECK_FAILED
    assert check.ok is True
    assert TestClient(gateway.app).get("/ready").status_code == 200


def test_a_defect_in_the_check_is_loud_even_though_it_does_not_gate(monkeypatch, caplog):
    """Not gating is not the same as not saying. It is the one state in which this gateway
    serves while knowing nothing about its own signature support."""
    monkeypatch.setattr(
        identity, "check_signature_support", lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    with caplog.at_level(logging.ERROR, logger="dag_tools.central_gateway"):
        identity.run_signature_self_check()
    assert any(r.levelno >= logging.ERROR for r in caplog.records)


def test_an_algorithm_this_check_cannot_mint_a_key_for_is_unexercised_not_unsupported(monkeypatch):
    """Registration means its backend imported. Refusing readiness over this check's own
    ignorance would hold a working gateway out of service the day the library grows a name."""
    invented = "ZZ512"
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", invented)
    registry = dict(_REGISTRY)
    registry[invented] = object()
    monkeypatch.setattr(jwt.algorithms, "get_default_algorithms", lambda: registry)
    check = identity.run_signature_self_check()
    assert check.unexercised == (invented,)
    assert check.unsupported == () and check.failed == ()
    assert check.ok is True


def test_control_an_algorithm_the_library_does_not_register_is_unsupported(monkeypatch):
    """The control for the test above: the SAME made-up name, left out of the registry.

    Without it, "unexercised" could be what this check calls everything it does not know, and
    the shipped fault would pass readiness.
    """
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "ZZ512")
    check = identity.run_signature_self_check()
    assert check.unsupported == ("ZZ512",)
    assert check.unexercised == ()
    assert check.ok is False


# ---------------------------------------------------------------------------
# What it costs, what it touches, what it says
# ---------------------------------------------------------------------------


def test_the_check_runs_once_at_startup_and_not_on_every_probe(monkeypatch):
    """A readiness probe runs every few seconds for the life of the pod. The answer cannot
    change under it: the library is in the image and the algorithm list is an environment
    variable, read once at startup, so a fix takes a restart -- which re-runs this."""
    calls = []
    real = identity.check_signature_support
    monkeypatch.setattr(
        identity, "check_signature_support", lambda: (calls.append(1), real())[1]
    )
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "HS256")
    monkeypatch.setattr(gateway.redis, "from_url", lambda *a, **k: _Redis())
    with TestClient(gateway.app) as client:
        for _ in range(3):
            assert client.get("/ready").status_code == 200
    assert calls == [1], "the key was minted again for a probe"


def test_the_lifespan_is_what_asks_the_question(monkeypatch):
    """The seal under `test_a_verdict_that_was_never_taken_does_not_gate`: that None never
    gates is only safe because production can never be holding one."""
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "HS256")
    monkeypatch.setattr(gateway.redis, "from_url", lambda *a, **k: _Redis())
    assert identity.signature_self_check() is None
    with TestClient(gateway.app):
        verdict = identity.signature_self_check()
    assert verdict is not None and verdict.verified == ("HS256",)


def test_only_the_configured_algorithms_are_exercised(monkeypatch):
    """A gateway configured for one symmetric algorithm must not pay for an RSA key pair."""
    minted = []
    real = rsa.generate_private_key
    monkeypatch.setattr(
        rsa, "generate_private_key", lambda *a, **k: (minted.append(1), real(*a, **k))[1]
    )
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "HS256")
    check = identity.run_signature_self_check()
    assert check.verified == ("HS256",)
    assert minted == []


def test_the_check_reaches_no_network(monkeypatch):
    """It asks what this PROCESS can do. A check that needed the issuer to be reachable would
    answer a different question, and would report somebody else's outage as an image fault."""
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")
    monkeypatch.setenv("GATEWAY_JWKS_URL", "https://idp.invalid/keys")

    def _no_sockets(*a, **k):
        raise AssertionError("the self-check opened a socket")

    monkeypatch.setattr(socket, "socket", _no_sockets)
    monkeypatch.setattr(socket, "create_connection", _no_sockets)
    check = identity.check_signature_support()
    assert check.verified == ("RS256",) and check.ok is True


def test_the_check_never_raises(monkeypatch):
    """Whatever the library does, this runs during startup; raising here would be a crash loop
    for a gateway that might well have been able to serve."""
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")

    def _boom():
        raise RuntimeError("the registry itself is broken")

    monkeypatch.setattr(jwt.algorithms, "get_default_algorithms", _boom)
    check = identity.check_signature_support()
    assert check.problem == identity.CHECK_FAILED


def test_the_published_verdict_names_no_key_material(monkeypatch):
    """The keys are minted in memory for this check and never leave it. The verdict travels
    into a readiness body, a health body and whatever scrapes either."""
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256,HS256")
    text = json.dumps(identity.run_signature_self_check().report())
    for marker in KEY_MATERIAL_MARKERS:
        assert marker not in text


def test_the_startup_line_names_no_key_material(monkeypatch, caplog):
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256,HS256")
    with caplog.at_level(logging.INFO, logger="dag_tools.central_gateway"):
        identity.run_signature_self_check()
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "RS256" in text, "the line says nothing at all"
    for marker in KEY_MATERIAL_MARKERS:
        assert marker not in text


# ---------------------------------------------------------------------------
# What this change may not touch
# ---------------------------------------------------------------------------


def test_the_refusal_vocabulary_is_unchanged(monkeypatch):
    """Ruled: this is a deployment fault, NOT a new refusal reason. Every reason token is an
    HTTP status and a number on `/health`, so adding one is a change to both."""
    assert identity.REASONS == REFUSAL_REASONS


def test_the_readiness_words_are_not_refusal_reasons(monkeypatch):
    """They describe the PROCESS; a reason describes a TOKEN. A reader must never be able to
    take one for the other, which is why none of them is spelled like `verification_unconfigured`."""
    words = {identity.LIBRARY_ABSENT, identity.NO_ALGORITHM_CONFIGURED, identity.CHECK_FAILED}
    assert words & set(identity.REASONS) == set()
    assert identity.NO_ALGORITHM_CONFIGURED != identity.UNCONFIGURED


def test_health_still_answers_200_when_the_gateway_is_not_ready(monkeypatch):
    """Liveness is not readiness. Restarting this pod fixes nothing -- the next process gets
    the same image and the same environment -- and a pod held out of service answers no probe
    an operator reads, so `/health` is where the verdict stays legible."""
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")
    monkeypatch.setattr(jwt.algorithms, "get_default_algorithms", lambda: _unavailable("RS256"))
    identity.run_signature_self_check()
    client = TestClient(gateway.app)
    assert client.get("/ready").status_code == 503
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["signature_support"]["ready"] is False
    # The gauges are still read, in their own blocks: this body carries three readings and one
    # of them failing must not take the others off it.
    assert "principal_record" in body and "refusal_reasons" in body


def test_health_reports_an_absent_verdict_as_null_rather_than_failing(monkeypatch):
    body = TestClient(gateway.app).get("/health").json()
    assert body["status"] == "ok"
    assert body["signature_support"] is None

"""What `enforce` verifies: the ISSUER against an accepted LIST and the AUDIENCE against one.

THE RULING these pin:
  1. Both are read from the deployment's own values. Neither has a default.
  2. An EMPTY list refuses everything. That state must be reachable, which is why there is no
     default -- a built-in value would make the rule unobservable rather than safe.
  3. The in-cluster name of an identity provider is a second accepted ENTRY, not a rewrite. A
     token carries the issuer it was minted with, and nothing here rewrites a proven claim.
  4. The singular env name is one entry of the list, so a deployment that set it keeps working
     and there is never a second resolution to disagree with the first.

These sit at the `identity` seam on purpose: the refusals' route-level consequence (Topaz is
never consulted, the broker is never called) is pinned by the parametrized refusal table in
`test_gateway_verified_identity.py`, which now carries the audience cases too.
"""
from __future__ import annotations

import logging
import re
import time
from pathlib import Path

import pytest

jwt = pytest.importorskip("jwt")
pytest.importorskip("cryptography", reason="RS256 tests need cryptography")

from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402

from dag_tools.central_gateway import identity  # noqa: E402

# Two spellings of ONE provider: a browser-facing URL and the name the same provider answers to
# from inside the cluster. Both are this test file's own invention; the library names neither.
ISS = "https://idp.example/realms/mesh"
IN_CLUSTER_ISS = "http://idp.idp.svc.cluster.local/realms/mesh"
AUD = "data-gateway"
OTHER_AUD = "some-other-client"
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


def tok(*, iss=ISS, aud=AUD, email="alice@example.com", exp=300):
    return jwt.encode(
        {"sub": "kc-1", "email": email, "iss": iss, "aud": aud, "exp": int(time.time()) + exp},
        PRIV, algorithm="RS256",
    )


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for k in (
        "GATEWAY_IDENTITY_POSTURE", "GATEWAY_JWT_ISSUER", "GATEWAY_JWT_ISSUERS",
        "GATEWAY_JWT_AUDIENCE", "GATEWAY_JWT_AUDIENCES", "GATEWAY_JWKS_URL",
        "GATEWAY_JWT_PUBLIC_KEY", "KEYCLOAK_PUBLIC_KEY", "GATEWAY_JWT_ALGORITHMS",
    ):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(identity, "_unconfigured_said", None)
    monkeypatch.setattr(identity, "_jwks_client", None)
    monkeypatch.setattr(identity, "_jwks_client_url", None)


@pytest.fixture
def verifying(monkeypatch):
    """The plural names, which is what a deployment configuring this today should set."""
    monkeypatch.setenv("GATEWAY_JWT_ISSUERS", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCES", AUD)
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", PUB)


def refusal(token):
    return identity.decide(token, None, None, ROUTE).refused


# ---------------------------------------------------------------------------
# The lists are read from the deployment, and nothing is written down here
# ---------------------------------------------------------------------------
def test_no_issuer_and_no_audience_is_compiled_in():
    """The empty state is the DEFAULT state, because a default value would hide the rule."""
    assert identity.accepted_issuers() == []
    assert identity.accepted_audiences() == []


def test_the_library_writes_down_no_issuer_and_no_audience_value():
    src = Path(identity.__file__).read_text(encoding="utf-8")
    code = "\n".join(
        line for line in src.splitlines()
        if not line.lstrip().startswith("#")
    )
    # A URL or a realm path in the module would be somebody's deployment, compiled in.
    assert not re.search(r"https?://\S", code), "an issuer-shaped value reached the module"


def test_a_list_is_comma_separated_trimmed_and_de_duplicated(monkeypatch):
    monkeypatch.setenv("GATEWAY_JWT_ISSUERS", f" {ISS} , {IN_CLUSTER_ISS},{ISS} ,")
    assert identity.accepted_issuers() == [ISS, IN_CLUSTER_ISS]


def test_the_singular_name_is_one_entry_of_the_list_not_a_second_resolution(monkeypatch):
    """Back-compat with no second answer: the two names UNION into one list."""
    monkeypatch.setenv("GATEWAY_JWT_ISSUERS", IN_CLUSTER_ISS)
    monkeypatch.setenv("GATEWAY_JWT_ISSUER", ISS)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCES", AUD)
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", PUB)
    assert identity.accepted_issuers() == [IN_CLUSTER_ISS, ISS]
    assert refusal(tok(iss=ISS)) is None
    assert refusal(tok(iss=IN_CLUSTER_ISS)) is None


# ---------------------------------------------------------------------------
# The issuer: a list, and the in-cluster name is an ENTRY of it
# ---------------------------------------------------------------------------
def test_a_second_accepted_issuer_admits_the_in_cluster_name(monkeypatch, verifying):
    """The failure this prevents: one provider, two names, and a pod refused for using the
    name its own cluster gave it."""
    assert refusal(tok(iss=IN_CLUSTER_ISS)) == identity.ISSUER_NOT_ACCEPTED
    monkeypatch.setenv("GATEWAY_JWT_ISSUERS", f"{ISS},{IN_CLUSTER_ISS}")
    assert refusal(tok(iss=IN_CLUSTER_ISS)) is None
    assert refusal(tok(iss=ISS)) is None, "listing a second entry must not unlist the first"


def test_an_unaccepted_issuer_is_refused_for_THAT_reason_not_as_an_invalid_token(verifying):
    """`token_invalid` would send an operator looking at keys and clocks. The token is fine."""
    assert refusal(tok(iss=IN_CLUSTER_ISS)) == identity.ISSUER_NOT_ACCEPTED
    assert identity._STATUS[identity.ISSUER_NOT_ACCEPTED] == 401


def test_the_refusal_names_the_rejected_issuer_and_the_var_to_list_it_in(verifying, caplog):
    caplog.set_level(logging.WARNING)
    token = tok(iss=IN_CLUSTER_ISS)
    assert refusal(token) == identity.ISSUER_NOT_ACCEPTED
    said = "\n".join(r.getMessage() for r in caplog.records)
    assert IN_CLUSTER_ISS in said, "the proven claim is the one diagnostic that closes this"
    assert identity.ISSUERS_ENV[0] in said
    assert token not in said


def test_the_issuer_is_not_delegated_to_pyjwt():
    """An older PyJWT compares a LIST to the claim as a string and refuses every token -- a total
    outage that reads as a key problem. The membership check stays here; see the module docstring.
    """
    src = Path(identity.__file__).read_text(encoding="utf-8")
    decode = re.search(r"claims = jwt\.decode\((?:[^()]|\([^()]*\))*\)", src)
    assert decode, "the decode call moved; this pin needs rewriting, not deleting"
    assert "issuer=" not in decode.group(0)


# ---------------------------------------------------------------------------
# The audience
# ---------------------------------------------------------------------------
def test_a_token_naming_another_audience_is_refused(verifying):
    assert refusal(tok(aud=OTHER_AUD)) == identity.TOKEN_INVALID


def test_a_token_with_no_audience_at_all_is_refused(verifying):
    assert refusal(tok(aud=None)) == identity.TOKEN_INVALID


def test_a_token_whose_audience_is_a_list_is_admitted_on_one_match(verifying):
    """A provider that puts several audiences on one token is ordinary. The intersection is
    PyJWT's, because getting it subtly wrong is how an audience check passes vacuously."""
    assert refusal(tok(aud=[OTHER_AUD, AUD])) is None
    assert refusal(tok(aud=[OTHER_AUD, "third-party"])) == identity.TOKEN_INVALID


def test_a_second_accepted_audience_does_not_unlist_the_first(monkeypatch, verifying):
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCES", f"{AUD},{OTHER_AUD}")
    assert refusal(tok(aud=AUD)) is None
    assert refusal(tok(aud=OTHER_AUD)) is None


# ---------------------------------------------------------------------------
# An empty list refuses EVERYTHING
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("unset", ["GATEWAY_JWT_ISSUERS", "GATEWAY_JWT_AUDIENCES"])
def test_an_empty_list_refuses_everything(unset, monkeypatch, verifying):
    monkeypatch.setenv(unset, "   ")  # present, and naming nothing
    assert refusal(tok()) == identity.UNCONFIGURED
    assert identity._STATUS[identity.UNCONFIGURED] == 503


def test_the_empty_list_is_checked_before_the_signature_so_the_reason_is_actionable(verifying,
                                                                                   monkeypatch):
    """Garbage in, `verification_unconfigured` out. The refusal has to name a cause an operator
    can act on; arriving as an indistinguishable `token_invalid` is a morning on the wrong
    suspect."""
    monkeypatch.delenv("GATEWAY_JWT_AUDIENCES")
    assert identity._verify("not.a.jwt")[1] == identity.UNCONFIGURED


def test_an_absent_token_is_still_told_apart_from_an_unconfigured_gateway(verifying, monkeypatch):
    monkeypatch.delenv("GATEWAY_JWT_ISSUERS")
    assert identity._verify("")[1] == identity.TOKEN_ABSENT


def test_the_unconfigured_refusal_names_what_is_unset_once_per_state(verifying, monkeypatch,
                                                                     caplog):
    """Latched on the STATE, not on having spoken: a deployment that is fixed and then broken
    again says so, and a quiet log never means a cleared one."""
    caplog.set_level(logging.ERROR)
    monkeypatch.delenv("GATEWAY_JWT_AUDIENCES")
    for _ in range(3):
        assert refusal(tok()) == identity.UNCONFIGURED
    said = [r.getMessage() for r in caplog.records if "unconfigured" in r.getMessage()]
    assert len(said) == 1 and identity.AUDIENCES_ENV[0] in said[0]
    monkeypatch.delenv("GATEWAY_JWT_ISSUERS")
    assert refusal(tok()) == identity.UNCONFIGURED
    said = [r.getMessage() for r in caplog.records if "unconfigured" in r.getMessage()]
    assert len(said) == 2 and identity.ISSUERS_ENV[0] in said[1]


# ---------------------------------------------------------------------------
# What the pod itself says about whether `enforce` can be turned on
# ---------------------------------------------------------------------------
def test_enforce_blockers_names_the_vars_and_nothing_else(verifying, monkeypatch):
    assert identity.enforce_blockers() == []
    monkeypatch.delenv("GATEWAY_JWT_AUDIENCES")
    assert identity.enforce_blockers() == [identity.AUDIENCES_ENV[0]]
    monkeypatch.delenv("GATEWAY_JWT_PUBLIC_KEY")
    blockers = identity.enforce_blockers()
    assert identity.AUDIENCES_ENV[0] in blockers
    assert any("GATEWAY_JWKS_URL" in b for b in blockers)
    assert not any(v in " ".join(blockers) for v in (ISS, AUD, PUB)), "names, never values"


def test_a_jwks_url_is_a_key_source_so_it_clears_that_blocker(monkeypatch, verifying):
    monkeypatch.delenv("GATEWAY_JWT_PUBLIC_KEY")
    monkeypatch.setenv("GATEWAY_JWKS_URL", "https://idp.example/jwks")
    assert identity.enforce_blockers() == []


def test_announce_counts_the_lists_and_says_whether_enforce_could_admit_anything(monkeypatch,
                                                                                verifying):
    monkeypatch.setenv("GATEWAY_IDENTITY_POSTURE", "enforce")
    monkeypatch.setenv("GATEWAY_JWT_ISSUERS", f"{ISS},{IN_CLUSTER_ISS}")
    line = identity.announce()
    assert "posture=enforce" in line and "issuers=2" in line and "audiences=1" in line
    assert "enforce_ready=yes" in line
    assert not any(v in line for v in (ISS, IN_CLUSTER_ISS, AUD, PUB)), "counts, never values"


def test_a_gateway_that_refuses_every_token_says_SO_rather_than_looking_quiet(monkeypatch):
    """`issuers=0` is the one state that otherwise reads exactly like a healthy quiet one."""
    monkeypatch.setenv("GATEWAY_IDENTITY_POSTURE", "enforce")
    line = identity.announce()
    assert "issuers=0" in line and "audiences=0" in line and "enforce_ready=no" in line
    assert identity.ISSUERS_ENV[0] in line and identity.AUDIENCES_ENV[0] in line

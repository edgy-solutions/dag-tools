"""The listing door: the assets a caller may read, and whether that answer is complete.

WHY THIS NEEDS A MODULE OF ITS OWN. Every other door on this gateway answers about ONE asset,
where a refusal and an outage are allowed to look the same: both deny, both 403, and ADR-0026's
posture says a gate that cannot reach its authorizer denies. A listing cannot borrow that
reading. "The authorizer refused every asset" and "the authorizer answered nothing" produce the
identical empty list, and an empty list reads as a sentence about the caller's entitlements --
`you may read nothing` -- when the fact is that this gateway could not find out. A caller
building a picker would show an empty picker and a user would file a bug against the wrong
system. So the body carries `complete` and the reason tokens behind it, and most of what is
pinned here is that distinction surviving.

WHAT IS PINNED, AND WHY EACH ONE COULD GO WRONG QUIETLY:

  * a permitted asset is named and a denied one is not -- the baseline, and the only assertion
    here that a broken route would fail loudly;
  * an authorizer that does not answer makes the response INCOMPLETE, and an allowed asset
    found alongside one that went undecided is still named. A listing that dropped the partial
    answer would be indistinguishable from a working one on a good day;
  * the body carries NO COUNTS and no masking rules. With the authorizer down the undecided
    count is the size of the routing table, which is a fact about the mesh, not the caller; and
    the authorizer answers with column lists and row filters, which are the shape of a read --
    handing them out here would publish the mesh's masking rules to anyone who can
    authenticate. The authorizer in these fixtures answers with both, so their absence from the
    body is measured rather than assumed;
  * the candidate cap and the deadline are EXACT, not merely safe in the right direction;
  * a deny-listed caller gets 403, the same answer the read door gives them, rather than an
    empty list -- a true sentence that reads as a different one;
  * an unreadable routing table is a 503 and not an empty list, for the same reason;
  * the route resolves under its OWN route id. The audit line names the route and the delegate
    table is keyed by it, so a listing recorded as a read authorization is a log describing a
    decision nobody made;
  * nothing mints a ticket and nothing calls the broker: the read door is the only way to
    anything behind it, and this door only answers whether that door would open.
"""
from __future__ import annotations

import asyncio
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
ALICE = "alice@example.com"
TOKEN_SUB = "kc-1"

#: Two assets in the routing table, so "named" and "not named" are both observable in one
#: response. A single-asset fixture cannot tell a listing from a yes/no.
OPEN = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/open,PROD)"
SHUT = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/shut,PROD)"

#: The masking rules the authorizer volunteers with every allow. They are here so their absence
#: from the response body is a measurement: a route that forwarded whatever the authorizer said
#: would carry these, and the assertion would fail.
COLUMNS = ["id", "amount"]
FILTERS = "region = 'x'"


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
        {
            "sub": TOKEN_SUB,
            "iss": ISS,
            "aud": AUD,
            "exp": int(time.time()) + 300,
            "email": email,
        },
        PRIV,
        algorithm="RS256",
    )


# ---------------------------------------------------------------------------
# The mesh the real route runs over
# ---------------------------------------------------------------------------
class _Resp:
    def __init__(self, status, body):
        self.status_code, self._b, self.text = status, body, ""

    def json(self):
        return self._b


class _Redis:
    """The routing table, and nothing else.

    `get` is present and recorded on purpose: that is the broker-url lookup the read door makes,
    so a listing that started minting tickets would show up here as a call rather than as an
    AttributeError that could be mistaken for a fixture gap.
    """

    def __init__(self, urns, explode=False):
        self._keys = [gateway.MESH_ROUTE_PREFIX + u for u in urns]
        self._explode = explode
        self.gets = []

    async def scan_iter(self, match=None, count=None):
        if self._explode:
            raise RuntimeError("routing table unavailable")
        for key in self._keys:
            yield key

    async def get(self, key):
        self.gets.append(key)
        return "http://broker.invalid"


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


def _mesh(monkeypatch, *, allow=(OPEN,), status=200, urns=(OPEN, SHUT), delay=0.0,
          explode=False):
    """The REAL route over a recorded authorizer and a recorded routing table."""
    seen = {"asked": [], "urls": []}
    redis = _Redis(urns, explode=explode)

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None, timeout=None):
            seen["urls"].append(url)
            urn = (json or {}).get("resource_context", {}).get("asset_key")
            seen["asked"].append(urn)
            if delay:
                await asyncio.sleep(delay)
            if status != 200:
                return _Resp(status, {})
            return _Resp(200, {
                "decisions": [{"is": urn in allow}],
                "allowed_columns": COLUMNS,
                "row_filters": FILTERS,
            })

    monkeypatch.setattr(gateway, "redis_client", redis)
    monkeypatch.setattr(gateway.httpx, "AsyncClient", lambda *a, **k: _Client())
    seen["redis"] = redis
    return seen


def _list(**headers):
    h = {"Authorization": f"Bearer {_tok()}"}
    h.update({k.replace("_", "-") if k.startswith("X") else k: v for k, v in headers.items()})
    return TestClient(gateway.app).get("/api/v1/assets", headers=h)


# ---------------------------------------------------------------------------
# The baseline
# ---------------------------------------------------------------------------
def test_a_permitted_asset_is_named_and_a_denied_one_is_not(monkeypatch):
    seen = _mesh(monkeypatch, allow=(OPEN,))
    body = _list().json()
    assert body["assets"] == [OPEN]
    assert body["complete"] is True
    assert body["incomplete_reasons"] == []
    # Anti-vacuity: the denied asset was actually PUT to the authorizer and came back a no. An
    # empty candidate set would satisfy the assertions above.
    assert sorted(seen["asked"]) == sorted([OPEN, SHUT])


def test_every_decision_is_the_authorizers_one_per_asset(monkeypatch):
    """No bulk shortcut and no reasoning of its own.

    A listing that worked permissions out for itself would be a second gate, and the two would
    disagree the first time a policy moved under one of them.
    """
    seen = _mesh(monkeypatch, allow=(OPEN, SHUT), urns=(OPEN, SHUT))
    _list()
    assert sorted(seen["asked"]) == sorted([OPEN, SHUT])
    assert all("/authz/is" in u for u in seen["urls"]), seen["urls"]


# ---------------------------------------------------------------------------
# Silence is not refusal
# ---------------------------------------------------------------------------
def test_an_authorizer_that_does_not_answer_makes_the_listing_incomplete(monkeypatch):
    """The whole reason this route carries `complete`.

    A non-200 is a deny on the read door and must not be one here: the empty list it would
    produce is the same empty list a caller with no entitlements gets, and nothing in the
    response would say which had happened.
    """
    seen = _mesh(monkeypatch, status=503)
    body = _list().json()
    assert body["assets"] == []
    assert body["complete"] is False
    assert gateway.LIST_UNDECIDED in body["incomplete_reasons"]
    # Anti-vacuity: the authorizer WAS asked, so the empty list is a silence and not an empty
    # routing table.
    assert seen["asked"], "nothing was asked, so this says nothing about an undecided answer"


def test_an_allowed_asset_alongside_an_undecided_one_is_still_named(monkeypatch):
    """A partial answer is published as a partial answer.

    Dropping it would make a degraded listing indistinguishable from a healthy one, which is
    the failure that reads as a success -- and keeping it silently complete would be worse.
    """
    asked = []

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None, timeout=None):
            urn = (json or {}).get("resource_context", {}).get("asset_key")
            asked.append(urn)
            if urn == SHUT:
                raise RuntimeError("authorizer unreachable")
            return _Resp(200, {"decisions": [{"is": True}]})

    monkeypatch.setattr(gateway, "redis_client", _Redis((OPEN, SHUT)))
    monkeypatch.setattr(gateway.httpx, "AsyncClient", lambda *a, **k: _Client())
    body = _list().json()
    assert body["assets"] == [OPEN]
    assert body["complete"] is False
    assert gateway.LIST_UNDECIDED in body["incomplete_reasons"]
    assert sorted(asked) == sorted([OPEN, SHUT])


def test_a_complete_listing_of_nothing_is_not_an_outage(monkeypatch):
    """The other side of the same coin: an empty routing table answers empty and COMPLETE.

    Without this, a route could pass every assertion above by reporting itself incomplete
    whenever the list came back short.
    """
    seen = _mesh(monkeypatch, urns=())
    body = _list().json()
    assert body["assets"] == []
    assert body["complete"] is True
    assert body["incomplete_reasons"] == []
    assert seen["asked"] == []


# ---------------------------------------------------------------------------
# What the body may not carry
# ---------------------------------------------------------------------------
def test_the_body_carries_no_counts_and_no_masking_rules(monkeypatch):
    """Three keys, and the authorizer's column list and row filter reach none of them.

    The count is withheld because with the authorizer down the undecided count is the size of
    the routing table -- how much the mesh holds, told to anyone who can authenticate. The
    masking rules are withheld because they are the shape of a read, and this door answers only
    whether a read would be allowed.
    """
    _mesh(monkeypatch, allow=(OPEN, SHUT))
    body = _list().json()
    assert set(body) == {"assets", "complete", "incomplete_reasons"}
    rendered = repr(body)
    for leaked in COLUMNS + [FILTERS]:
        assert leaked not in rendered, f"the response carries {leaked!r} from the authorizer"
    assert not [v for v in body.values() if isinstance(v, int) and not isinstance(v, bool)], body
    # Anti-vacuity: the authorizer really did answer with the masking rules this asserts are
    # absent, and really did allow both assets, so there was something to leak.
    assert body["assets"] == sorted([OPEN, SHUT])


# ---------------------------------------------------------------------------
# The cost bounds, exactly
# ---------------------------------------------------------------------------
def test_the_candidate_cap_is_exact_and_says_it_was_reached(monkeypatch):
    """Exact, not merely safe in the right direction.

    One candidate past the cap is collected and dropped, so the flag means what it says. A cap
    that stopped one short would set the flag on a listing that was in fact complete, and a
    caller cannot check that.
    """
    monkeypatch.setattr(gateway, "ASSET_LIST_MAX_CANDIDATES", 1)
    seen = _mesh(monkeypatch, allow=(OPEN, SHUT))
    body = _list().json()
    assert len(seen["asked"]) == 1, seen["asked"]
    assert len(body["assets"]) == 1
    assert body["complete"] is False
    assert gateway.LIST_TRUNCATED in body["incomplete_reasons"]


def test_the_cap_is_not_reached_when_the_table_fits(monkeypatch):
    """The control. Without it, a route that always truncated would pass the test above."""
    monkeypatch.setattr(gateway, "ASSET_LIST_MAX_CANDIDATES", 2)
    seen = _mesh(monkeypatch, allow=(OPEN, SHUT))
    body = _list().json()
    assert len(seen["asked"]) == 2
    assert body["complete"] is True
    assert gateway.LIST_TRUNCATED not in body["incomplete_reasons"]


def test_an_authorizer_that_is_too_slow_makes_the_listing_incomplete(monkeypatch):
    """A decision that was never asked is not a denial, and it is not an undecided answer
    either: it has its own reason, because the fix is a different one -- the deadline or the
    concurrency bound, not the authorizer.
    """
    monkeypatch.setattr(gateway, "ASSET_LIST_DEADLINE_SECONDS", 0.05)
    seen = _mesh(monkeypatch, allow=(OPEN, SHUT), delay=5.0)
    body = _list().json()
    assert body["complete"] is False
    assert gateway.LIST_DEADLINE in body["incomplete_reasons"]
    # Anti-vacuity: the deadline fired on a request that had actually started.
    assert seen["asked"], "nothing was asked, so the deadline is not what this measured"


def test_a_bad_bound_warns_and_defaults_rather_than_crash_looping_the_pod(monkeypatch):
    """A typo in a tuning knob must not be a pod that will not start.

    These are cost bounds, not correctness ones: every value still produces a correct listing,
    so refusing to boot over one trades a slow listing for no gateway at all.
    """
    monkeypatch.setenv("ASSET_LIST_MAX_CANDIDATES", "not-a-number")
    assert gateway._positive_int("ASSET_LIST_MAX_CANDIDATES", 500) == 500
    monkeypatch.setenv("ASSET_LIST_MAX_CANDIDATES", "0")
    assert gateway._positive_int("ASSET_LIST_MAX_CANDIDATES", 500) == 500
    monkeypatch.setenv("ASSET_LIST_MAX_CANDIDATES", "7")
    assert gateway._positive_int("ASSET_LIST_MAX_CANDIDATES", 500) == 7


# ---------------------------------------------------------------------------
# Refusals: the same answer the read door gives
# ---------------------------------------------------------------------------
def test_a_deny_listed_caller_is_refused_rather_than_handed_an_empty_list(monkeypatch):
    """403, exactly as the read door answers them.

    An empty list would be a true sentence read as a different one -- it says "you may read
    nothing", which is an answer about entitlements, when the fact is that this door is shut to
    them. The two doors saying the same thing is what makes the deny list mean one thing.
    """
    monkeypatch.setattr(gateway, "DENIED_USER_SUBS", {TOKEN_SUB})
    seen = _mesh(monkeypatch, allow=(OPEN, SHUT))
    resp = _list()
    assert resp.status_code == 403
    # Anti-vacuity: refused BEFORE the authorizer was consulted, so the 403 is the deny list's
    # and not an empty listing dressed up.
    assert seen["asked"] == []


def test_a_routing_table_that_cannot_be_read_is_a_503_not_an_empty_list(monkeypatch):
    """Same reasoning one layer out: an unreadable table is not an empty table."""
    _mesh(monkeypatch, explode=True)
    resp = _list()
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# The route names itself, and mints nothing
# ---------------------------------------------------------------------------
def test_the_listing_resolves_under_its_own_route_id(monkeypatch):
    """`assets.list`, never `assets.authorize`.

    The audit line names the route, so a shared id records a listing as a read authorization --
    evidence of a decision nobody made. And the delegate table is keyed per route, so a shared
    id silently widens every existing row to a route nobody granted.
    """
    routes = []
    real = identity.resolve_principal

    def _recording(token, on_behalf_of, legacy, route, urn):
        routes.append(route)
        return real(token, on_behalf_of, legacy, route, urn)

    monkeypatch.setattr(identity, "resolve_principal", _recording)
    _mesh(monkeypatch, allow=(OPEN,))
    assert _list().status_code == 200
    assert routes == [identity.ROUTE_LIST]
    assert identity.ROUTE_LIST != identity.ROUTE_AUTHORIZE


def test_the_listing_mints_no_ticket_and_calls_no_broker(monkeypatch):
    """The read door is the only way to anything behind it.

    This door answers whether that door would open, which means it never resolves a broker, so
    nothing it does can hand out a credential or a physical uri.
    """
    seen = _mesh(monkeypatch, allow=(OPEN, SHUT))
    body = _list().json()
    assert seen["redis"].gets == [], "the listing looked up a broker url"
    assert all("/authz/is" in u for u in seen["urls"]), seen["urls"]
    rendered = repr(body)
    for forbidden in ("physical_uri", "credential", "ticket", "source_type"):
        assert forbidden not in rendered, f"the listing published {forbidden}"
    # Anti-vacuity: the listing did run and did answer.
    assert body["assets"] == sorted([OPEN, SHUT])


# ---------------------------------------------------------------------------
# The read door's contract is untouched
# ---------------------------------------------------------------------------
def test_check_topaz_authz_still_answers_the_tuple_other_repos_import(monkeypatch):
    """AGENTS.md §6: other repos import this function.

    The listing needed the undecided/allowed distinction `check_topaz_authz` discards, so the
    body moved to `topaz_decision` and this stayed as the read door's view of it -- same name,
    same signature, same three-tuple.
    """
    _mesh(monkeypatch, allow=(OPEN,))
    decision = identity.IdentityDecision(ALICE, None, None, None, TOKEN_SUB)

    allowed, columns, filters = asyncio.run(
        gateway.check_topaz_authz("t", OPEN, decision)
    )
    assert (allowed, columns, filters) == (True, COLUMNS, FILTERS)

    denied = asyncio.run(gateway.check_topaz_authz("t", SHUT, decision))
    assert denied == (False, COLUMNS, FILTERS)


def test_a_silence_reaches_the_read_door_as_a_deny_and_the_listing_as_undecided(monkeypatch):
    """One answer, two readings, and the readings must stay different.

    The read door is right to treat a silence as a deny (ADR-0026: authz is a gate). The
    listing is right not to. If `topaz_decision` ever stopped carrying the distinction, both
    would still deny and only the listing would be wrong -- quietly, on a response body that
    looks exactly like a correct one.
    """
    _mesh(monkeypatch, status=503)
    decision = identity.IdentityDecision(ALICE, None, None, None, TOKEN_SUB)

    outcome = asyncio.run(gateway.topaz_decision("t", OPEN, decision))
    assert outcome.undecided is True
    assert outcome.allowed is False
    assert outcome.cause, "an undecided outcome carries no cause, so the log cannot say why"

    assert asyncio.run(gateway.check_topaz_authz("t", OPEN, decision)) == (False, None, None)

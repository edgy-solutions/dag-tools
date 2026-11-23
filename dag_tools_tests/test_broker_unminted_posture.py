"""A reader never receives a credential this broker did not mint.

WHAT THESE LOCK DOWN. invincible-agent ADR-0044's rollout is per backend, so a
ticket for a backend with no minter was served as-is -- and
``_sanitize_ticket`` deliberately puts the producer's echoed ``credentials``
back on exactly those tickets, because for an unminted backend that credential
is the only way to read. The exposure was counted
(``UNPROTECTED_SOURCE_TYPES``) and labelled
(``mode: "producer-credential-unprotected"``), which is how it was found.
Counting an exposure is not closing it.

So, as in ``test_broker_mints_ticket_credentials.py``, these assert on the
CONSUMER's side of the seam: what ``resolve_asset`` hands back, not what a
producer advertised.

EVERY SEAL HAS A CONTROL, because each one here would pass for the wrong
reason on its own. "Enforce refuses" passes on a broker that refuses
everything; its control is a minted read served under the same posture. "An
unminted ticket with no credential is served" passes on a guard that never
fires; its control is the same path carrying a credential. And the structural
checks pass vacuously on a function they cannot see into, so each plants the
defect it looks for.
"""
from __future__ import annotations

import ast
import asyncio
import json
import pathlib
from typing import Any, Dict

import pytest

from dag_tools.domain_broker import main as broker
from dag_tools.domain_broker import unminted_posture


PRODUCER_TOKEN = "PRODUCER_WRITE_TOKEN"

# A source_type outside the five the mesh protocol knows, which is what a
# duck-typed third-party IO manager may advertise. Named for what it is rather
# than after any product: the point is only that no minter claims it.
UNMINTED_TYPE = "unminted_backend"

URN = "urn:li:dataset:(urn:li:dataPlatform:s3,x,PROD)"


# -- doubles ---------------------------------------------------------------

class _UnmintedProducer:
    """A producer on a backend the broker has no minter for.

    Returning its own credential is not a mistake on its part: with no minter
    there is nothing else that would let a reader read. That is the exposure,
    and it is the producer's credential -- a write credential, unscoped, not
    expiring with the access window.
    """

    def __init__(self, source_type=UNMINTED_TYPE, credentials=None, mode=None):
        self.source_type = source_type
        self.credentials = {"token": PRODUCER_TOKEN} if credentials is None else credentials
        self.mode = mode

    def physical_coordinates(self, asset_key_path):
        ticket: Dict[str, Any] = {"physical_uri": "proto://store.example.com/thing/"}
        if self.source_type is not None:
            ticket["source_type"] = self.source_type
        if self.credentials:
            ticket["credentials"] = self.credentials
        if self.mode is not None:
            ticket["mode"] = self.mode
        return ticket


class _MintedProducer:
    """A producer on a backend the broker DOES mint for: the control's control."""

    def physical_coordinates(self, asset_key_path):
        return {
            "source_type": "s3_parquet",
            "physical_uri": "s3://a-lake/a/b/",
            "credentials": {
                "aws_access_key_id": "PRODUCER_WRITE_KEY",
                "aws_endpoint_url": "http://store.example.com:9000",
                "aws_region": "us-east-1",
            },
        }


class _Record:
    def __init__(self, family="s3_parquet"):
        self.asset_key = ["a", "b"]
        self.io_manager_key = "io_manager"
        self.io_manager_family = family
        self.io_manager_class = "third_party.io.SomeIOManager"
        self.tags = {}
        self.urn = URN


class _FakeSTS:
    def assume_role(self, **kwargs):
        return {
            "Credentials": {
                "AccessKeyId": "MINTED_KEY",
                "SecretAccessKey": "MINTED_SECRET",
                "SessionToken": "MINTED_TOKEN",
            }
        }


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    broker.LOCAL_ASSETS.clear()
    broker.ECHOED_CREDENTIALS_DROPPED.clear()
    broker.UNPROTECTED_SOURCE_TYPES.clear()
    broker.NON_FQDN_HOSTS.clear()
    broker.AS_OF_ABSENT.clear()
    unminted_posture.WOULD_REFUSE.clear()
    unminted_posture.REFUSED.clear()
    monkeypatch.delenv(unminted_posture.POSTURE_ENV, raising=False)
    monkeypatch.setattr(broker.boto3, "client", lambda *a, **k: _FakeSTS())
    yield


def _seed(producer, family="s3_parquet"):
    broker.LOCAL_ASSETS[URN] = broker._build_asset_info_from_record(
        _Record(family=family), io_manager=producer
    )


def _resolve():
    return asyncio.run(broker.resolve_asset(broker.ResolveRequest(urn=URN)))


# -- the posture: observe serves, enforce refuses --------------------------

def test_observe_serves_exactly_what_it_served_before():
    """The default, and the control for every refusal below.

    Refusing by default would break the data access of a deployment part-way
    through its rollout, so the default is today's behaviour exactly, plus a
    number. If this test ever fails, a deployment that merely upgraded the
    library lost reads it had, which AGENTS.md section 6 does not permit.
    """
    _seed(_UnmintedProducer())

    ticket = _resolve()

    assert ticket["credentials"]["token"] == PRODUCER_TOKEN
    assert unminted_posture.WOULD_REFUSE == {UNMINTED_TYPE: 1}
    assert unminted_posture.REFUSED == {}, (
        "nothing is refused under observe; a count here means a read was "
        "refused and reported as merely observed"
    )


def test_enforce_refuses_and_the_reader_receives_no_credential(monkeypatch):
    """The point of the change. Its control is the test above: same producer,
    same asset, served under the default posture."""
    monkeypatch.setenv(unminted_posture.POSTURE_ENV, "enforce")
    _seed(_UnmintedProducer())

    with pytest.raises(broker.HTTPException) as excinfo:
        _resolve()

    assert excinfo.value.status_code == 403
    assert PRODUCER_TOKEN not in json.dumps(excinfo.value.detail)
    assert unminted_posture.REFUSED == {UNMINTED_TYPE: 1}
    assert unminted_posture.WOULD_REFUSE == {UNMINTED_TYPE: 1}, (
        "under enforce the two numbers are one decision, so a refusal counts "
        "in both -- the one state in which their agreeing is correct"
    )


def test_the_refusal_body_carries_the_reason_and_nothing_else(monkeypatch):
    """Not the env var, not the physical uri, not the backend. Those go to the
    broker's log, where no reader sees them; the body is the reason token."""
    monkeypatch.setenv(unminted_posture.POSTURE_ENV, "enforce")
    _seed(_UnmintedProducer())

    with pytest.raises(broker.HTTPException) as excinfo:
        _resolve()

    assert excinfo.value.detail == {"error": unminted_posture.REASON}


def test_a_minted_ticket_is_never_refused_under_enforce(monkeypatch):
    """Enforce is about credentials the broker did not create. A deployment
    that flips the posture must not lose the reads invincible-agent ADR-0044
    already protects -- and without this, "enforce refuses" would pass on a
    broker that refused everything."""
    monkeypatch.setenv(unminted_posture.POSTURE_ENV, "enforce")
    _seed(_MintedProducer())

    ticket = _resolve()

    assert ticket["credentials"]["aws_access_key_id"] == "MINTED_KEY"
    assert unminted_posture.WOULD_REFUSE == {}
    assert unminted_posture.REFUSED == {}


def test_an_unminted_ticket_with_no_credential_is_served_and_counted_as_nothing(monkeypatch):
    """Nothing is exposed, so there is nothing to refuse.

    A coordinates-only route for an unminted backend is a working read of
    public or ambiently-authorised data, and refusing it would cost a
    deployment that read for no gain. Its control is the refusal tests above:
    the same path, the same posture, a credential present."""
    monkeypatch.setenv(unminted_posture.POSTURE_ENV, "enforce")
    _seed(_UnmintedProducer(credentials={}))

    ticket = _resolve()

    assert not ticket.get("credentials")
    assert unminted_posture.WOULD_REFUSE == {}
    assert unminted_posture.REFUSED == {}


# -- what the seam may NOT decide from -------------------------------------

def test_a_producer_cannot_pass_an_unminted_ticket_off_as_minted(monkeypatch):
    """``mode`` is the producer's word and survives the sanitiser.

    ``_sanitize_ticket`` writes ``clean.get("mode") or ...``, so a producer
    advertising ``mint-sts`` on a backend with no minter keeps that label --
    and would keep its credentials too, if ``mode`` were what the guard read.
    """
    monkeypatch.setenv(unminted_posture.POSTURE_ENV, "enforce")
    _seed(_UnmintedProducer(mode="mint-sts"))

    assert broker.LOCAL_ASSETS[URN]["_routing_ticket"]["mode"] == "mint-sts", (
        "the spoofed mode did not survive into the cache, so this test would "
        "prove nothing about the guard"
    )

    with pytest.raises(broker.HTTPException) as excinfo:
        _resolve()
    assert excinfo.value.status_code == 403


def test_the_legacy_database_path_is_not_read_as_minted(monkeypatch):
    """``source_type`` is not where a credential came from either.

    That path returns a hardcoded placeholder token for ``postgres`` and
    ``clickhouse``, and both ARE in ``_MINTERS`` -- so a guard reading
    membership would call that token a minted credential and serve it as
    protected. It is reachable: an asset with a ``datahub/urn`` tag and no
    protocol IO manager lands in ``LOCAL_ASSETS`` without a routing ticket.
    """
    monkeypatch.setenv(unminted_posture.POSTURE_ENV, "enforce")
    assert "postgres" in broker._MINTERS, (
        "the premise of this test: membership would have read as minted"
    )
    broker.LOCAL_ASSETS[URN] = {"io_manager_type": "postgres"}

    with pytest.raises(broker.HTTPException) as excinfo:
        _resolve()

    assert excinfo.value.status_code == 403
    assert unminted_posture.REFUSED == {"postgres": 1}


def test_control_the_legacy_object_store_path_mints_and_is_served(monkeypatch):
    """Control for the test above, which alone would pass on a guard that
    refused every path with no routing ticket. This one has no ticket either,
    and mints."""
    monkeypatch.setenv(unminted_posture.POSTURE_ENV, "enforce")
    broker.LOCAL_ASSETS[URN] = {"io_manager_type": "s3_parquet"}

    ticket = _resolve()

    assert ticket["credentials"]["aws_access_key_id"] == "MINTED_KEY"
    assert unminted_posture.REFUSED == {}


def test_a_ticket_naming_no_backend_counts_under_a_visible_key():
    """The dangerous sub-case, and it needs a key an operator can read.

    ``_MINTERS.get("")`` misses, so a ticket with no ``source_type`` is
    unminted -- which is right -- but counted under ``""`` it reads on
    ``/health`` as a rendering artefact rather than as a backend nobody can
    name.
    """
    _seed(_UnmintedProducer(source_type=None))

    _resolve()

    assert unminted_posture.WOULD_REFUSE == {unminted_posture.ABSENT_SOURCE_TYPE: 1}
    assert "" not in unminted_posture.WOULD_REFUSE


def test_an_unrecognised_posture_fails_closed(monkeypatch):
    """A typo must not read as the permissive posture. Same grammar and same
    reading as every other posture switch in this repo, which is why it is
    ``parse_posture`` rather than a second parser."""
    monkeypatch.setenv(unminted_posture.POSTURE_ENV, "enfroce")
    _seed(_UnmintedProducer())

    with pytest.raises(broker.HTTPException):
        _resolve()


# -- the guard runs before the ticket is issued ----------------------------

def test_a_refused_read_is_not_counted_as_a_ticket_that_was_issued(monkeypatch):
    """``AS_OF_ABSENT`` counts tickets a consumer cannot pin to a version. A
    read that was refused issued no ticket, so counting it there would report
    an identity gap for a read that never happened."""
    monkeypatch.setenv(unminted_posture.POSTURE_ENV, "enforce")
    _seed(_UnmintedProducer())

    with pytest.raises(broker.HTTPException):
        _resolve()

    assert broker.AS_OF_ABSENT == {}


def test_control_the_same_read_served_is_counted_there():
    """Without this, the test above would pass on a broker that had stopped
    counting as-of absence altogether."""
    _seed(_UnmintedProducer())

    _resolve()

    assert broker.AS_OF_ABSENT == {UNMINTED_TYPE: 1}


# -- an instrument may never break the thing it measures -------------------

class _BadCounter(dict):
    def get(self, *args, **kwargs):
        raise RuntimeError("planted counter defect")


def test_a_defect_in_the_counter_does_not_refuse_the_read(monkeypatch):
    monkeypatch.setattr(unminted_posture, "WOULD_REFUSE", _BadCounter())
    _seed(_UnmintedProducer())

    ticket = _resolve()

    assert ticket["credentials"]["token"] == PRODUCER_TOKEN


def test_a_defect_in_the_counter_does_not_rescue_a_refused_read(monkeypatch):
    """The other direction: the decision is taken before anything is counted,
    so a broken counter must not turn a refusal into a service."""
    monkeypatch.setenv(unminted_posture.POSTURE_ENV, "enforce")
    monkeypatch.setattr(unminted_posture, "WOULD_REFUSE", _BadCounter())
    _seed(_UnmintedProducer())

    with pytest.raises(broker.HTTPException):
        _resolve()


class _BadMapping:
    def keys(self):
        raise RuntimeError("planted reporting defect")


def _loaded(monkeypatch):
    monkeypatch.setattr(broker, "DEFINITIONS_LOADED", True)
    monkeypatch.setattr(broker, "DEFINITIONS_ERROR", None)


def test_health_answers_rather_than_raising_when_a_counter_misbehaves(monkeypatch):
    """A readiness probe that 500s because a counter misbehaved takes the pod
    out over an instrument."""
    _loaded(monkeypatch)
    monkeypatch.setattr(unminted_posture, "REFUSED", _BadMapping())

    assert unminted_posture.health() == {
        "posture": None, "would_refuse": None, "refused": None,
    }

    whole = asyncio.run(broker.health())
    assert whole["adr0044"]["unminted_credentials"]["refused"] is None


def test_health_publishes_both_numbers_separately(monkeypatch):
    """Never one number: under observe a would-refusal is SERVED, so a sum
    would report refusals for reads that were served."""
    _loaded(monkeypatch)
    _seed(_UnmintedProducer())
    _resolve()

    reported = asyncio.run(broker.health())["adr0044"]["unminted_credentials"]

    assert reported["posture"] == unminted_posture.POSTURE_OBSERVE
    assert reported["would_refuse"] == {UNMINTED_TYPE: 1}
    assert reported["refused"] == {}


def test_the_log_line_names_no_credential(caplog):
    caplog.set_level("WARNING")
    _seed(_UnmintedProducer())

    _resolve()

    assert unminted_posture.REASON in caplog.text
    assert PRODUCER_TOKEN not in caplog.text


# -- structural: the shape that keeps the guard at the one seam ------------

_SOURCE = pathlib.Path(broker.__file__).read_text(encoding="utf-8")
_TREE = ast.parse(_SOURCE)


def _function(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise AssertionError("no function named " + name)


def _returns_not_declaring(func):
    """Return statements that do not say whether the broker minted."""
    bad = []
    for node in ast.walk(func):
        if isinstance(node, ast.Return) and node.value is not None:
            call = node.value
            declared = (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Name)
                and call.func.id == "Resolved"
            )
            if not declared:
                bad.append(node.lineno)
    return bad


def test_every_resolve_path_declares_whether_the_broker_minted():
    """The seam cannot infer it, so each path says.

    A path added later that returns a bare dict fails here, and fails loudly
    at runtime on ``.ticket`` -- rather than being read as minted, which is
    the one failure that would look like protection.
    """
    func = _function(_TREE, "_resolve_ticket")
    bad = _returns_not_declaring(func)

    assert bad == [], (
        "returns at lines " + repr(bad) + " of _resolve_ticket do not declare "
        "whether the broker minted the credential they carry"
    )
    valued = [n for n in ast.walk(func)
              if isinstance(n, ast.Return) and n.value is not None]
    assert len(valued) >= 4, (
        "fewer return paths than this function has ever had; the check has "
        "lost sight of some"
    )


_PLANTED = '''
async def _resolve_ticket(request):
    if request:
        return Resolved({}, minted=True)
    return {"credentials": {"token": "x"}}
'''


def test_control_the_check_finds_a_return_that_does_not_declare():
    """Without this, the test above would pass on a checker that looked at
    nothing."""
    bad = _returns_not_declaring(_function(ast.parse(_PLANTED), "_resolve_ticket"))
    assert len(bad) == 1


def test_the_guard_decides_before_the_ticket_is_stamped():
    """A refused read must not be counted as a ticket that was issued; the
    runtime half of that is above, and this pins the ordering it rests on."""
    func = _function(_TREE, "resolve_asset")
    seen = {}
    for node in ast.walk(func):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr in ("decide", "stamp"):
                seen.setdefault(node.func.attr, node.lineno)

    assert "decide" in seen and "stamp" in seen, repr(seen)
    assert seen["decide"] < seen["stamp"]


def test_the_seam_has_exactly_one_caller():
    """``resolve_asset`` is the one seam every ticket leaves through (AGENTS.md
    section 4o). A second caller of ``_resolve_ticket`` is a second way out,
    past both the guard and the stamp."""
    callers = [
        node for node in ast.walk(_TREE)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_resolve_ticket"
    ]
    assert len(callers) == 1, repr([n.lineno for n in callers])

    seam = _function(_TREE, "resolve_asset")
    assert seam.lineno < callers[0].lineno <= seam.end_lineno


# -- the gateway forwards the refusal as a refusal -------------------------
#
# A refused read reaches the consumer through `classify_response`, which reads
# 403 as `NotEntitled` and everything else as `MeshUnavailable`. Reported as a
# 502 the refusal arrives as "the gateway could not serve the request", which
# tells the reader to retry and the operator to go and look at the broker's
# health -- both the wrong place, and the one failure mode that makes a posture
# flip look like an outage.

def _gateway(broker_status, broker_body):
    pytest.importorskip("redis", reason="gateway tests need the [broker] extra")
    import jwt
    from fastapi.testclient import TestClient

    from dag_tools.central_gateway import main as gateway

    class _Redis:
        async def get(self, key):
            return "http://broker.invalid"

    class _Resp:
        def __init__(self, code, body):
            self.status_code, self._b = code, body
            self.text = json.dumps(body)

        def json(self):
            return self._b

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None, headers=None, timeout=None):
            if "/authz/is" in url:
                return _Resp(200, {"decisions": [{"is": True}]})
            return _Resp(broker_status, broker_body)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(gateway, "redis_client", _Redis())
    monkeypatch.setattr(gateway.httpx, "AsyncClient", lambda *a, **k: _Client())
    monkeypatch.setattr(gateway, "DENIED_USER_SUBS", set())
    monkeypatch.delenv("GATEWAY_IDENTITY_POSTURE", raising=False)

    token = jwt.encode({"sub": "kc-1", "email": "svc@example.com"}, "k" * 32,
                       algorithm="HS256")
    try:
        response = TestClient(gateway.app).post(
            "/api/v1/assets/" + URN + "/authorize",
            headers={"Authorization": "Bearer " + token,
                     "on_behalf_of": "alice@example.com"},
        )
    finally:
        monkeypatch.undo()
    return response


def test_a_refused_read_reaches_the_reader_as_a_refusal():
    response = _gateway(403, {"error": unminted_posture.REASON})

    assert response.status_code == 403
    assert response.json()["detail"] == {"error": unminted_posture.REASON}


def test_control_a_broker_fault_is_still_a_bad_gateway():
    """Without this, the test above would pass on a gateway that forwarded
    every broker status, which would report a broker's own 500 as a refusal of
    the caller -- the same mistake in the other direction."""
    response = _gateway(500, {"detail": "broker fell over"})

    assert response.status_code == 502


def test_nothing_of_the_brokers_body_but_the_reason_reaches_the_reader():
    """The body for this status carries a reason token by construction, but the
    gateway is what a reader talks to: it forwards the one key it means to, so
    an operator detail added to that body later does not travel with it."""
    response = _gateway(403, {"error": unminted_posture.REASON,
                              "hint": "set BROKER_UNMINTED_POSTURE=observe",
                              "physical_uri": "proto://store.example.com/thing/"})

    assert response.json()["detail"] == {"error": unminted_posture.REASON}
    assert "store.example.com" not in response.text

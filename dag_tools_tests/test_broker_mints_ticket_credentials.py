"""ADR-0044 — a routing ticket carries only credentials the broker minted.

WHY THESE TESTS EXIST. The defect they lock down was not a missing feature: the
scoped-STS minting had been implemented in ``resolve_asset`` since the fallback
path was written. A newer path — the mesh-publishing protocol — cached the IO
manager's own ticket at load time and returned it verbatim, short-circuiting
past the minting code sitting in the same function. A regression by
convergence, and nothing in the suite noticed, because every test asserted on
the ticket a producer PRODUCED rather than the credential a consumer RECEIVED.

So these assert on the consumer's side of the seam.

THE LOAD-TIME TRAP, tested explicitly (test_no_producer_credential_is_cached).
``physical_coordinates()`` runs ONCE at broker startup and its result lives in
``LOCAL_ASSETS`` for the process lifetime. A credential minted there would be
one credential shared by every caller, expiring an hour into a broker still
reporting ``{"status": "ok"}`` — strictly worse than the echoed credential it
replaced, and invisible to any test written against a freshly started broker.
The fix depends on minting happening per REQUEST, so that is what is asserted.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Dict

import pytest

from dag_tools.domain_broker import main as broker


# ── doubles ────────────────────────────────────────────────────────────────

class _EchoingIOManager:
    """A producer on the OLD contract — returns its own writing credential.

    Deliberately not updated to the new protocol: the broker must protect
    assets published by IO managers that have not yet been upgraded, or the
    fix would wait on the whole fleet moving at once.
    """

    def physical_coordinates(self, asset_key_path):
        return {
            "source_type": "s3_parquet",
            "physical_uri": "s3://publog-lake/publog/p_cage/",
            "credentials": {
                "aws_access_key_id": "PRODUCER_WRITE_KEY",
                "aws_secret_access_key": "PRODUCER_WRITE_SECRET",
                "aws_endpoint_url": "http://minio-svc:9000",
                "aws_region": "us-east-1",
            },
        }


class _Record:
    def __init__(self):
        self.asset_key = ["minio-svc", "publog-lake", "publog", "p_cage"]
        self.io_manager_key = "io_manager"
        self.io_manager_family = "s3_parquet"
        self.io_manager_class = "dag_tools.io_managers.arrow.ConfigurableArrowIOManager"
        self.tags = {}
        self.urn = "urn:li:dataset:(urn:li:dataPlatform:s3,x,PROD)"


class _FakeSTS:
    """Captures the inline policy so the SCOPE can be asserted, not just the call."""

    last_call: Dict[str, Any] = {}

    def assume_role(self, **kwargs):
        _FakeSTS.last_call = kwargs
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
    _FakeSTS.last_call = {}
    monkeypatch.setattr(broker.boto3, "client", lambda *a, **k: _FakeSTS())
    yield


# ── the load-time half: nothing secret survives into the cache ─────────────

def test_no_producer_credential_is_cached():
    """The cache holds coordinates. A secret here would be a process-lifetime leak."""
    info = broker._build_asset_info_from_record(_Record(), io_manager=_EchoingIOManager())
    cached = info["_routing_ticket"]

    assert "credentials" not in cached, (
        "a producer credential reached LOCAL_ASSETS, where it would sit in the "
        "broker's memory for the process lifetime and be re-sent on every resolve"
    )
    blob = json.dumps(cached)
    assert "PRODUCER_WRITE_KEY" not in blob
    assert "PRODUCER_WRITE_SECRET" not in blob


def test_coordinates_survive_the_strip():
    """Dropping the whole credentials dict would take the endpoint with it."""
    info = broker._build_asset_info_from_record(_Record(), io_manager=_EchoingIOManager())
    cached = info["_routing_ticket"]

    assert cached["endpoint_url"] == "http://minio-svc:9000"
    assert cached["region"] == "us-east-1"
    assert cached["physical_uri"] == "s3://publog-lake/publog/p_cage/"


def test_echoed_credential_is_counted_by_producer():
    """The retirement counter — the hard break lands when this reads zero."""
    broker._build_asset_info_from_record(_Record(), io_manager=_EchoingIOManager())
    assert broker.ECHOED_CREDENTIALS_DROPPED == {
        "dag_tools.io_managers.arrow.ConfigurableArrowIOManager": 1
    }


def test_namespace_local_host_is_reported():
    """A ticket is consumed elsewhere; `minio-svc` means nothing there."""
    broker._build_asset_info_from_record(_Record(), io_manager=_EchoingIOManager())
    assert "minio-svc" in broker.NON_FQDN_HOSTS


# ── the request-time half: what a consumer actually receives ───────────────

def test_consumer_receives_a_minted_credential_not_the_producers():
    """THE acceptance criterion. Every other check passes while this one fails."""
    urn = "urn:li:dataset:(urn:li:dataPlatform:s3,publog,PROD)"
    broker.LOCAL_ASSETS[urn] = broker._build_asset_info_from_record(
        _Record(), io_manager=_EchoingIOManager()
    )

    ticket = asyncio.run(broker.resolve_asset(broker.ResolveRequest(urn=urn)))

    assert ticket["credentials"]["aws_access_key_id"] == "MINTED_KEY"
    assert ticket["credentials"]["aws_session_token"] == "MINTED_TOKEN"
    assert "PRODUCER_WRITE_KEY" not in json.dumps(ticket)


def test_minted_policy_is_scoped_to_the_asset_and_cannot_write():
    """Read-only, this prefix. Asserted on the policy, not on the call happening."""
    urn = "urn:li:dataset:(urn:li:dataPlatform:s3,publog,PROD)"
    broker.LOCAL_ASSETS[urn] = broker._build_asset_info_from_record(
        _Record(), io_manager=_EchoingIOManager()
    )

    asyncio.run(broker.resolve_asset(broker.ResolveRequest(urn=urn)))
    policy = json.loads(_FakeSTS.last_call["Policy"])

    actions = {a for s in policy["Statement"] for a in s["Action"]}
    assert actions == {"s3:GetObject", "s3:ListBucket"}, (
        "a ticket credential must not be able to write; found " + repr(actions)
    )

    get_stmt = next(s for s in policy["Statement"] if s["Action"] == ["s3:GetObject"])
    assert get_stmt["Resource"] == ["arn:aws:s3:::publog-lake/publog/p_cage/*"], (
        "the credential reaches the whole bucket instead of this asset's prefix"
    )
    assert _FakeSTS.last_call["DurationSeconds"] > 0


def test_each_request_mints_afresh():
    """Guards the load-time trap: two callers must not share one credential."""
    urn = "urn:li:dataset:(urn:li:dataPlatform:s3,publog,PROD)"
    broker.LOCAL_ASSETS[urn] = broker._build_asset_info_from_record(
        _Record(), io_manager=_EchoingIOManager()
    )

    calls = []
    original = _FakeSTS.assume_role

    def counting(self, **kwargs):
        calls.append(kwargs)
        return original(self, **kwargs)

    _FakeSTS.assume_role = counting
    try:
        asyncio.run(broker.resolve_asset(broker.ResolveRequest(urn=urn)))
        asyncio.run(broker.resolve_asset(broker.ResolveRequest(urn=urn)))
    finally:
        _FakeSTS.assume_role = original

    assert len(calls) == 2, (
        "the credential was minted once and cached — the load-time trap, which "
        "expires mid-flight while /health still reports ok"
    )


def test_minting_failure_fails_closed():
    """A minting failure must never reinstate the producer credential."""
    urn = "urn:li:dataset:(urn:li:dataPlatform:s3,publog,PROD)"
    broker.LOCAL_ASSETS[urn] = broker._build_asset_info_from_record(
        _Record(), io_manager=_EchoingIOManager()
    )

    def boom(*a, **k):
        raise RuntimeError("STS unavailable")

    original = _FakeSTS.assume_role
    _FakeSTS.assume_role = boom
    try:
        with pytest.raises(Exception) as excinfo:
            asyncio.run(broker.resolve_asset(broker.ResolveRequest(urn=urn)))
    finally:
        _FakeSTS.assume_role = original

    assert "PRODUCER_WRITE_KEY" not in str(excinfo.value)


# ── the broker's own minting identity ──────────────────────────────────────
#
# ADR-0044 gave the broker minting authority and did not say where its
# credentials come from. The first implementation left boto3 to its default
# chain, which a domain-broker pod does not populate — it carries S3_ACCESS_KEY
# / S3_SECRET_KEY, names boto3 has never heard of. Every mint raised
# NoCredentialsError and every read 503'd: a configuration problem reported in
# the vocabulary of an outage. These pin the resolution so it cannot regress to
# ambient.

def test_sts_identity_prefers_the_purpose_named_pair(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        broker.boto3, "client",
        lambda *a, **kw: captured.update(kw) or _FakeSTS(),
    )
    monkeypatch.setenv("BROKER_STS_ACCESS_KEY_ID", "broker-key")
    monkeypatch.setenv("BROKER_STS_SECRET_ACCESS_KEY", "broker-secret")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ambient-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")

    broker._sts_client({"endpoint_url": "http://minio:9000", "region": "us-east-1"})

    assert captured["aws_access_key_id"] == "broker-key"


def test_sts_identity_accepts_the_pods_existing_s3_vars(monkeypatch):
    """An already-deployed broker must work after upgrading, with no helm change."""
    captured = {}
    monkeypatch.setattr(
        broker.boto3, "client",
        lambda *a, **kw: captured.update(kw) or _FakeSTS(),
    )
    for var in ("BROKER_STS_ACCESS_KEY_ID", "BROKER_STS_SECRET_ACCESS_KEY",
                "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("S3_ACCESS_KEY", "minio-sandbox")
    monkeypatch.setenv("S3_SECRET_KEY", "minio-sandbox-secret")

    broker._sts_client({"endpoint_url": "http://minio:9000", "region": "us-east-1"})

    assert captured["aws_access_key_id"] == "minio-sandbox", (
        "a broker configured the way every domain-broker pod is configured "
        "would have found no identity and 503'd every read"
    )


def test_no_identity_configured_falls_through_to_ambient(monkeypatch):
    """Last rung: IRSA / instance profiles must still work — pass nothing."""
    captured = {}
    monkeypatch.setattr(
        broker.boto3, "client",
        lambda *a, **kw: captured.update(kw) or _FakeSTS(),
    )
    for var in ("BROKER_STS_ACCESS_KEY_ID", "BROKER_STS_SECRET_ACCESS_KEY",
                "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
                "S3_ACCESS_KEY", "S3_SECRET_KEY"):
        monkeypatch.delenv(var, raising=False)

    broker._sts_client({"region": "us-east-1"})

    assert "aws_access_key_id" not in captured

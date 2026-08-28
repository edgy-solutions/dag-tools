"""A NON-dag-tools IO manager can publish to the mesh, end to end.

WHY THE DOUBLE IS DELIBERATELY FOREIGN. The classes here inherit from nothing
in dag_tools except the mixin, and live in no dag_tools module — because the
question these tests answer is whether a THIRD PARTY'S manager works, and a
double built from dag-tools' own base classes could pass while the real case
fails. That gap is not hypothetical: a deployment was found advertising 142
assets and serving none, because its IO managers were vendored copies of
dag-tools' — same class names, no shared ancestry, and no
``physical_coordinates`` because they were forked before the protocol existed.

The last test walks the whole path — mixin -> load-time sanitize -> resolve ->
minted credential — because every stage in between has, at some point today,
been the one that silently dropped the asset.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from dag_tools.domain_broker import main as broker
from dag_tools.io_managers import MeshPublishable


class ForeignArrowManager(MeshPublishable):
    """What a consumer's own IO manager looks like after adopting the mixin."""

    uri_base = "s3://staging"
    endpoint = "http://minio-svc.d4-sandbox.svc.cluster.local:9000"

    def mesh_uri(self, asset_key_path):
        return "/".join([self.uri_base.rstrip("/"), *asset_key_path]) + "/"

    def mesh_endpoint(self):
        return self.endpoint


class LocalOnlyManager(MeshPublishable):
    """Writes to the pod's local disk — has nothing to advertise."""

    def mesh_uri(self, asset_key_path):
        return None


class ExoticManager(MeshPublishable):
    def mesh_uri(self, asset_key_path):
        return "s3://bucket/thing/"

    def mesh_source_type(self, asset_key_path):
        return "parquet_over_carrier_pigeon"


class _Record:
    def __init__(self, key):
        self.asset_key = key
        self.io_manager_key = "io_manager"
        self.io_manager_family = "s3_parquet"
        self.io_manager_class = "orch.resources.arrow.ConfigurableArrowIOManager"
        self.tags = {}
        self.urn = "urn:li:dataset:(urn:li:dataPlatform:dagster,x,PROD)"


class _FakeSTS:
    last_call: dict = {}

    def assume_role(self, **kwargs):
        _FakeSTS.last_call = kwargs
        return {"Credentials": {"AccessKeyId": "MINTED", "SecretAccessKey": "S",
                                "SessionToken": "T"}}


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    broker.LOCAL_ASSETS.clear()
    broker.ECHOED_CREDENTIALS_DROPPED.clear()
    broker.UNPROTECTED_SOURCE_TYPES.clear()
    broker.NON_FQDN_HOSTS.clear()
    broker.UNADVERTISED_ASSETS.clear()
    _FakeSTS.last_call = {}
    monkeypatch.setattr(broker.boto3, "client", lambda *a, **k: _FakeSTS())
    monkeypatch.setenv("S3_ACCESS_KEY", "k")
    monkeypatch.setenv("S3_SECRET_KEY", "s")
    yield


# ── the mixin's own contract ───────────────────────────────────────────────

def test_one_hook_produces_a_complete_ticket():
    """mesh_uri() is the only thing a foreign manager has to write."""
    ticket = ForeignArrowManager().physical_coordinates(["vdspc_axi", "dbo", "board_mapping"])

    assert ticket["source_type"] == "s3_parquet"
    assert ticket["physical_uri"] == "s3://staging/vdspc_axi/dbo/board_mapping/"
    assert ticket["mode"] == "mint-sts"
    assert ticket["scope"] == {"bucket": "staging", "prefix": "vdspc_axi/dbo/board_mapping"}


def test_the_mixin_offers_no_way_to_supply_credentials():
    """Structural, not documentary — ADR-0044 says the broker mints."""
    ticket = ForeignArrowManager().physical_coordinates(["a", "b"])
    assert "credentials" not in ticket
    assert not any("credential" in h for h in dir(MeshPublishable))


def test_declining_is_a_first_class_answer():
    """An advertised-but-unreadable location is worse than no advertisement."""
    assert LocalOnlyManager().physical_coordinates(["a"]) is None
    assert ForeignArrowManager().physical_coordinates([]) is None


def test_unknown_source_type_is_refused_at_the_producer():
    """The client raises ValueError on dispatch — too late, and in the wrong process."""
    assert ExoticManager().physical_coordinates(["a"]) is None


# ── the whole path, which is the point ─────────────────────────────────────

def test_foreign_manager_reaches_a_minted_credential():
    urn = "urn:li:dataset:(urn:li:dataPlatform:s3,staging.vdspc_axi/dbo/board_mapping,PROD)"
    record = _Record(["vdspc_axi", "dbo", "board_mapping"])

    info = broker._build_asset_info_from_record(record, io_manager=ForeignArrowManager())
    broker.LOCAL_ASSETS[urn] = info

    ticket = asyncio.run(broker.resolve_asset(broker.ResolveRequest(urn=urn)))

    assert ticket["credentials"]["aws_access_key_id"] == "MINTED"
    policy = json.loads(_FakeSTS.last_call["Policy"])
    get_stmt = next(s for s in policy["Statement"] if s["Action"] == ["s3:GetObject"])
    assert get_stmt["Resource"] == [
        "arn:aws:s3:::staging/vdspc_axi/dbo/board_mapping/*"
    ], "the minted credential is not confined to this foreign manager's asset"


def test_fqdn_endpoint_passes_the_broker_check():
    """The mixin's docstring tells producers to use the FQDN; verify it lands."""
    record = _Record(["a", "b"])
    broker._build_asset_info_from_record(record, io_manager=ForeignArrowManager())
    assert broker.NON_FQDN_HOSTS == {}, (
        "an FQDN endpoint was still reported as namespace-local"
    )


def test_a_manager_without_the_protocol_is_not_advertised():
    """The mfg case: a fork predating physical_coordinates()."""
    class VendoredFork:  # no mixin, no method — exactly what was found
        pass

    reason = broker._unadvertisable_reason(_Record(["a"]), VendoredFork())
    assert "physical_coordinates" in reason
    assert "MeshPublishable" in reason or "Configurable" in reason, (
        "the reason must tell the operator what to DO, not only what is missing"
    )

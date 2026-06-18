"""Tests for ``check_probes_status`` — the cross-reference between the
probe manifest and the test deployment's dag-tools-probes location.

Two layers:
  * Pure cross-reference tests against ``_build_report`` (no I/O).
  * End-to-end tests with a moto-backed registry + mocked GraphQL client.
"""
import json
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")

import boto3
from moto import mock_aws

from dag_tools.inventory import SCHEMA_VERSION as INV_VER
from dag_tools.qual.classes import build_class_matrix, publish_class_matrix
from dag_tools.qual.graphql import (
    CodeLocationStatus,
    DagsterGraphQLClient,
    DagsterGraphQLError,
)
from dag_tools.qual.probes import check_probes_status
from dag_tools.qual.probes.runner import PROBES_LOCATION_NAME
from dag_tools.qual.probes.status import _build_report
from dag_tools.qual.qualify import (
    Deployment,
    VersionTarget,
    create_qualification,
)
from dag_tools.qual.registry import (
    BuildMeta,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
    layout,
)
from dag_tools.qual.synthetic import (
    ProbeManifest,
    ProbeModule,
    generate_bundle,
    publish_bundle,
)


BUCKET = "dag-tools-probes-status-test"


# ---------------------------------------------------------------------------
# Cross-reference unit tests — pure logic, no I/O
# ---------------------------------------------------------------------------


def _manifest(probes_spec):
    """Build a minimal ProbeManifest from [(class_hash, module_name), ...]."""
    probes = [
        ProbeModule(
            class_hash=ch, module_name=mn,
            source_key=f"qualifications/q/probes/{ch}.py",
        )
        for ch, mn in probes_spec
    ]
    return ProbeManifest(
        qual_id="q", generated_at=datetime.now(tz=timezone.utc),
        synthetic_class_count=len(probes), probes=probes,
    )


def test_build_report_fully_loaded():
    pm = _manifest([("hashA", "probe_aaaaaaaa")])
    report = _build_report(
        qual_id="q", probe_manifest=pm,
        location_status="LOADED", location_error=None,
        location_asset_keys=[["probe_aaaaaaaa_upstream"], ["probe_aaaaaaaa_downstream"]],
    )
    assert report.fully_loaded_class_count == 1
    assert report.fully_loaded_class_hashes == ["hashA"]
    assert report.missing_class_hashes == []
    assert report.partially_loaded == []
    assert report.all_loaded is True


def test_build_report_missing_when_no_assets_loaded():
    pm = _manifest([("hashA", "probe_aaaaaaaa")])
    report = _build_report(
        qual_id="q", probe_manifest=pm,
        location_status="LOADED", location_error=None,
        location_asset_keys=[],
    )
    assert report.fully_loaded_class_count == 0
    assert report.missing_class_hashes == ["hashA"]
    assert report.all_loaded is False


def test_build_report_partially_loaded():
    """Operator's manual edit broke the downstream — upstream still
    imports, downstream doesn't. We want this to be VISIBLE not hidden."""
    pm = _manifest([("hashA", "probe_aaaaaaaa")])
    report = _build_report(
        qual_id="q", probe_manifest=pm,
        location_status="LOADED", location_error=None,
        location_asset_keys=[["probe_aaaaaaaa_upstream"]],
    )
    assert report.fully_loaded_class_count == 0
    assert len(report.partially_loaded) == 1
    p = report.partially_loaded[0]
    assert p.class_hash == "hashA"
    assert p.upstream_loaded is True
    assert p.downstream_loaded is False
    assert report.all_loaded is False


def test_build_report_unexpected_asset_keys():
    """Stale `<class_hash>.py` from a prior bundle still in
    DAGTOOLS_PROBES_DIR; the deployment loaded its assets too."""
    pm = _manifest([("hashA", "probe_aaaaaaaa")])
    report = _build_report(
        qual_id="q", probe_manifest=pm,
        location_status="LOADED", location_error=None,
        location_asset_keys=[
            ["probe_aaaaaaaa_upstream"],
            ["probe_aaaaaaaa_downstream"],
            ["probe_deadbeef_upstream"],     # stale
            ["probe_deadbeef_downstream"],   # stale
            ["normal_user_asset"],            # not probe-shaped → ignored
        ],
    )
    assert sorted(report.unexpected_probe_asset_keys) == [
        ["probe_deadbeef_downstream"],
        ["probe_deadbeef_upstream"],
    ]
    # Fully-loaded still counts the expected ones; unexpected is informational.
    assert report.fully_loaded_class_count == 1
    # all_loaded is True — the recipe says unexpected is "informational",
    # not a deploy-state failure. Operators clean it up if they care.
    assert report.all_loaded is True


def test_build_report_location_absent():
    """Location not registered in the workspace at all — every expected
    probe is missing and the operator sees ABSENT in the location status."""
    pm = _manifest([("hashA", "probe_aaaaaaaa")])
    report = _build_report(
        qual_id="q", probe_manifest=pm,
        location_status="ABSENT", location_error=None,
        location_asset_keys=[],
    )
    assert report.location_load_status == "ABSENT"
    assert report.missing_class_hashes == ["hashA"]
    assert report.all_loaded is False


def test_build_report_location_error_state():
    """ERROR state — operator-actionable error attached, every probe
    counted as missing because we couldn't query them."""
    pm = _manifest([("hashA", "probe_aaaaaaaa")])
    report = _build_report(
        qual_id="q", probe_manifest=pm,
        location_status="ERROR", location_error="syntax error in foo.py",
        location_asset_keys=[],
    )
    assert report.location_error == "syntax error in foo.py"
    assert report.all_loaded is False


# ---------------------------------------------------------------------------
# End-to-end: moto registry + mocked GraphQL
# ---------------------------------------------------------------------------


def _publish_asset_inventory(reg, repo, sha, *, asset_key, io_manager_class,
                              io_manager_family, tags=None):
    when = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    rec = {
        "schema_version": INV_VER,
        "asset_key": list(asset_key),
        "compute_kind": "python",
        "io_manager_key": "io_manager",
        "io_manager_class": io_manager_class,
        "io_manager_family": io_manager_family,
        "partitions_def_class": None,
        "partition_mapping_classes": [],
        "resource_keys": ["io_manager"],
        "resource_classes": {"io_manager": io_manager_class},
        "integration_libs": [],
        "has_asset_checks": False,
        "automation_condition_type": None,
        "tags": tags or {},
    }
    artifacts = {
        layout.ASSETS_FILE: json.dumps({
            "schema_version": 1,
            "inventory_schema_version": INV_VER,
            "records": [rec],
        }).encode("utf-8"),
        layout.AUTOMATION_FILE: b'{"schema_version":1,"sensors":[],"schedules":[],"asset_checks":[]}',
        layout.IO_MANAGERS_FILE: b'{"schema_version":1,"entries":[]}',
        layout.DBT_PROJECTS_FILE: b'{"schema_version":1,"projects":[]}',
        layout.LOAD_VALIDATION_FILE: b'{"schema_version":1,"timestamp":"2026-06-15T12:00:00+00:00","loads":true,"locations":[],"failures":[],"warnings":[]}',
    }
    reg.publish_build(
        repo=repo, git_sha=sha, artifacts=artifacts,
        meta=BuildMeta(repo=repo, git_sha=sha, timestamp=when),
    )


def _seed_qual_with_probes(reg, qual_id="q-test"):
    _publish_asset_inventory(
        reg, "alpha", "shaA",
        asset_key=["x"],
        io_manager_class="dagster_snowflake.SnowflakeIOManager",
        io_manager_family="snowflake",
        tags={"synthetic_required": "true"},
    )
    create_qualification(
        qual_id=qual_id, registry=reg,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://dagster-test/graphql"),
    )
    matrix = build_class_matrix(qual_id, registry=reg)
    publish_class_matrix(matrix, registry=reg)
    bundle = generate_bundle(qual_id, registry=reg)
    publish_bundle(bundle, registry=reg)
    return qual_id, bundle


def _fake_client(loaded_keys: List[List[str]],
                 location_state: str = "LOADED",
                 location_error: Optional[str] = None) -> DagsterGraphQLClient:
    client = MagicMock(spec=DagsterGraphQLClient)
    client.get_code_locations.return_value = [
        CodeLocationStatus(
            name=PROBES_LOCATION_NAME,
            load_status=location_state,
            error=location_error,
        ),
    ]
    client.get_location_asset_keys.return_value = loaded_keys
    client.close.return_value = None
    return client


@pytest.fixture
def setup(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))


def test_e2e_status_reports_fully_loaded(setup):
    reg = setup
    qual_id, bundle = _seed_qual_with_probes(reg)
    probe = bundle.manifest.probes[0]
    client = _fake_client([
        [f"{probe.module_name}_upstream"],
        [f"{probe.module_name}_downstream"],
    ])

    report = check_probes_status(
        qual_id, registry=reg, client_factory=lambda manifest: client,
    )
    assert report.all_loaded is True
    assert report.fully_loaded_class_hashes == [probe.class_hash]


def test_e2e_status_reports_missing(setup):
    reg = setup
    qual_id, bundle = _seed_qual_with_probes(reg)
    client = _fake_client([])

    report = check_probes_status(
        qual_id, registry=reg, client_factory=lambda manifest: client,
    )
    assert report.missing_class_hashes == [bundle.manifest.probes[0].class_hash]
    assert report.all_loaded is False


def test_e2e_status_handles_absent_location(setup):
    """The dag-tools-probes location isn't registered in the workspace
    at all → ABSENT + missing probes."""
    reg = setup
    qual_id, bundle = _seed_qual_with_probes(reg)
    client = MagicMock(spec=DagsterGraphQLClient)
    client.get_code_locations.return_value = []  # no locations at all
    client.close.return_value = None

    report = check_probes_status(
        qual_id, registry=reg, client_factory=lambda manifest: client,
    )
    assert report.location_load_status == "ABSENT"
    assert report.missing_class_hashes == [bundle.manifest.probes[0].class_hash]
    # We did NOT call get_location_asset_keys when location is absent —
    # the safety guard short-circuits the query.
    client.get_location_asset_keys.assert_not_called()


def test_e2e_status_raises_when_probe_manifest_missing(setup):
    reg = setup
    _publish_asset_inventory(
        reg, "alpha", "shaA",
        asset_key=["x"],
        io_manager_class="dagster.InMemoryIOManager",
        io_manager_family="in_memory",
    )
    create_qualification(
        qual_id="q-test", registry=reg,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://dagster-test/graphql"),
    )
    with pytest.raises(FileNotFoundError, match="no probe manifest"):
        check_probes_status(
            "q-test", registry=reg,
            client_factory=lambda manifest: _fake_client([]),
        )

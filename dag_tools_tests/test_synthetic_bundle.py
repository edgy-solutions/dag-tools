"""End-to-end tests for the Q5 probe bundle orchestrator."""
import json
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("yaml")

import boto3
from moto import mock_aws

from dag_tools.inventory import SCHEMA_VERSION as INV_VER
from dag_tools.qual.qualify import VersionTarget, create_qualification
from dag_tools.qual.classes import build_class_matrix, publish_class_matrix
from dag_tools.qual.registry import (
    BuildMeta,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
    ImmutableKeyExists,
    layout,
)
from dag_tools.qual.synthetic import (
    ProbeManifest,
    generate_bundle,
    publish_bundle,
    write_local_bundle,
    default_local_probes_dir,
)


BUCKET = "dag-tools-qual-synthetic-test"


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _publish_asset(registry, repo, sha, *,
                   asset_key, io_manager_class,
                   io_manager_family,
                   integration_libs=None,
                   tags=None):
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
        "integration_libs": integration_libs or [],
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
    registry.publish_build(
        repo=repo, git_sha=sha, artifacts=artifacts,
        meta=BuildMeta(repo=repo, git_sha=sha, timestamp=when),
    )


def _setup_qual_with_classes(registry, qual_id="q-test"):
    """Two repos:
      * alpha — runnable class (in-memory IO manager, no tag).
      * beta  — synthetic-required class (snowflake FQN + the
        ``synthetic_required: "true"`` tag the classifier reads).
    """
    _publish_asset(
        registry, "alpha", "shaA",
        asset_key=["x"],
        io_manager_class="dagster.InMemoryIOManager",
        io_manager_family="in_memory",
    )
    _publish_asset(
        registry, "beta", "shaB",
        asset_key=["y"],
        io_manager_class="dagster_snowflake.SnowflakeIOManager",
        io_manager_family="snowflake",
        tags={"synthetic_required": "true"},
    )
    create_qualification(
        qual_id=qual_id, registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
    )
    matrix = build_class_matrix(qual_id, registry=registry)
    publish_class_matrix(matrix, registry=registry)
    return matrix


# ---------------------------------------------------------------------------
# generate_bundle
# ---------------------------------------------------------------------------


def test_generate_bundle_emits_probe_for_synthetic_class_only():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        matrix = _setup_qual_with_classes(reg)

        bundle = generate_bundle("q-test", registry=reg)

        synthetic_total = sum(
            1 for cls in matrix.classes
            if cls.representatives
            and cls.representatives[0].runnability.value == "synthetic_required"
        )
        assert bundle.manifest.synthetic_class_count == synthetic_total
        assert len(bundle.manifest.probes) == synthetic_total
        # Sources keyed by class_hash
        for probe in bundle.manifest.probes:
            assert probe.class_hash in bundle.sources
            assert bundle.sources[probe.class_hash].startswith('"""')


def test_generate_bundle_includes_io_manager_fqn_in_source():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _setup_qual_with_classes(reg)
        bundle = generate_bundle("q-test", registry=reg)
        assert len(bundle.manifest.probes) >= 1
        any_source = next(iter(bundle.sources.values()))
        assert "from dagster_snowflake import SnowflakeIOManager" in any_source


def test_generate_bundle_raises_when_manifest_missing():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        with pytest.raises(FileNotFoundError, match="no qualification manifest"):
            generate_bundle("never-existed", registry=reg)


def test_generate_bundle_raises_when_class_matrix_missing():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        # Manifest exists but no class matrix yet.
        _publish_asset(
            reg, "alpha", "shaA",
            asset_key=["x"],
            io_manager_class="dagster.InMemoryIOManager",
            io_manager_family="in_memory",
        )
        create_qualification(
            qual_id="q-test", registry=reg,
            baseline=VersionTarget(dagster="1.10.6"),
            candidate=VersionTarget(dagster="1.12.1"),
        )
        with pytest.raises(FileNotFoundError, match="no equivalence-class matrix"):
            generate_bundle("q-test", registry=reg)


# ---------------------------------------------------------------------------
# publish_bundle — invariant: sources first, manifest LAST
# ---------------------------------------------------------------------------


def test_publish_bundle_writes_sources_then_manifest():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _setup_qual_with_classes(reg)
        bundle = generate_bundle("q-test", registry=reg)
        publish_bundle(bundle, registry=reg)

        # Every probe's source landed in the registry under the expected key.
        for probe in bundle.manifest.probes:
            stored = reg.read_probe_source("q-test", probe.class_hash)
            assert stored is not None
            assert stored.decode("utf-8") == bundle.sources[probe.class_hash]

        # And the manifest itself.
        manifest_body = reg.read_probe_manifest("q-test")
        assert manifest_body is not None
        parsed = ProbeManifest.model_validate_json(manifest_body)
        assert parsed.qual_id == "q-test"
        assert parsed.synthetic_class_count == bundle.manifest.synthetic_class_count


def test_publish_bundle_is_immutable_by_default():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _setup_qual_with_classes(reg)
        bundle = generate_bundle("q-test", registry=reg)
        publish_bundle(bundle, registry=reg)

        # Re-publish without --allow-overwrite raises.
        with pytest.raises(ImmutableKeyExists):
            publish_bundle(bundle, registry=reg)


def test_publish_bundle_allow_overwrite_succeeds():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _setup_qual_with_classes(reg)
        bundle = generate_bundle("q-test", registry=reg)
        publish_bundle(bundle, registry=reg)
        publish_bundle(bundle, registry=reg, allow_overwrite=True)
        assert reg.read_probe_manifest("q-test") is not None


# ---------------------------------------------------------------------------
# write_local_bundle
# ---------------------------------------------------------------------------


def test_write_local_bundle_creates_one_file_per_probe_plus_manifest(tmp_path):
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        reg = InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))
        _setup_qual_with_classes(reg)
        bundle = generate_bundle("q-test", registry=reg)

        out = write_local_bundle(bundle, local_path=tmp_path / "probes")
        assert out.is_dir()
        assert (out / "probe_manifest.json").exists()
        for probe in bundle.manifest.probes:
            assert (out / f"{probe.class_hash}.py").exists()
        # Manifest body matches the in-memory bundle.
        manifest_body = (out / "probe_manifest.json").read_text(encoding="utf-8")
        assert json.loads(manifest_body)["qual_id"] == "q-test"


def test_default_local_probes_dir_honors_dagtools_home(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    out = default_local_probes_dir("q-test")
    assert out == tmp_path / "quals" / "q-test" / "probes"
